import json
import logging
import math
import sys
from abc import abstractmethod
from typing import Iterable, Optional, Union, cast, List

import datahub.emitter.mce_builder as builder
import pandas as pd
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit import UsageStatsWorkUnit
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.redash_state import RedashCheckpointState
from datahub.ingestion.source.state.stateful_ingestion_base import (
    JobId,
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.container import ContainerProperties
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    ChangeTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    StatusClass,
    StringTypeClass,
    ContainerClass,
    BrowsePathsClass,
    DatasetPropertiesClass,
    KafkaSchemaClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    SubTypesClass,
)

from pydantic.fields import Field
from redash_toolbelt import Redash
from requests.adapters import HTTPAdapter
from urllib3 import Retry

logger = logging.getLogger(__name__)


class IBRedashSourceStatefulIngestionConfig(StatefulIngestionConfig):
    remove_stale_metadata: bool = True


class RedashSourceReport(StatefulIngestionReport):
    workunits_deleted: int = 0


class IBRedashSourceConfig(StatefulIngestionConfigBase):
    connect_uri: str = Field(
        default="http://localhost:5000", description="Redash base URL."
    )
    api_key: str = Field(default="REDASH_API_KEY", description="Redash user API key.")
    query_id: str = Field(
        default="QUERY_ID",
        description="Target redash query",
    )
    api_page_limit: int = Field(
        default=sys.maxsize,
        description="Limit on number of pages queried for ingesting dashboards and charts API "
                    "during pagination. ",
    )
    stateful_ingestion: Optional[IBRedashSourceStatefulIngestionConfig] = None


class IBRedashSource(StatefulIngestionSourceBase):
    batch_size = 1000
    config: IBRedashSourceConfig
    client: Redash
    report: RedashSourceReport

    def __init__(self, config: IBRedashSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: IBRedashSourceConfig = config
        self.report: RedashSourceReport = RedashSourceReport()

        self.config.connect_uri = self.config.connect_uri.strip("/")
        self.client = Redash(self.config.connect_uri, self.config.api_key)
        self.client.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        # Handling retry and backoff
        retries = 3

        backoff_factor = 10
        status_forcelist = (500, 503, 502, 504)
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )

        adapter = HTTPAdapter(max_retries=retry)
        self.client.session.mount("http://", adapter)
        self.client.session.mount("https://", adapter)

        self.api_page_limit = self.config.api_page_limit or math.inf

    @classmethod
    def create(cls, config_dict, ctx):
        config = IBRedashSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def query_get(self, query_id) -> str:
        url = f"//api/queries/{query_id}/results"
        return self.client._post(url).json()["query_result"]["data"]["rows"]

    @abstractmethod
    def fetch_workunits(self) -> Iterable[WorkUnit]:
        raise NotImplementedError("Sub-classes must implement this method.")

    def get_workunits(self) -> Iterable[WorkUnit]:
        if not self.is_stateful_ingestion_configured():
            for wu in self.fetch_workunits():
                self.report.workunits_produced += 1
                yield wu
            return

        cur_checkpoint = self.get_current_checkpoint(
            self.get_default_ingestion_job_id()
        )
        cur_checkpoint_state = (
            cast(RedashCheckpointState, cur_checkpoint.state)
            if cur_checkpoint is not None
            else None
        )

        for wu in self.fetch_workunits():
            self.report.workunits_produced += 1
            if type(wu) is not MetadataWorkUnit:
                yield wu
                continue
            wu.metadata.proposedSnapshot.aspects.append(StatusClass(removed=False))
            if cur_checkpoint_state is not None:
                cur_checkpoint_state.add_urn(wu.metadata.proposedSnapshot.urn)
            yield wu

        last_checkpoint = self.get_last_checkpoint(
            self.get_default_ingestion_job_id(), RedashCheckpointState
        )
        last_checkpoint_state = (
            cast(RedashCheckpointState, last_checkpoint.state)
            if last_checkpoint is not None
            else None
        )
        if (
                self.config.stateful_ingestion
                and self.config.stateful_ingestion.remove_stale_metadata
                and last_checkpoint_state is not None
                and cur_checkpoint_state is not None
        ):
            logger.info("deleting")
            for urn in last_checkpoint_state.get_urns_not_in(cur_checkpoint_state):
                self.report.workunits_deleted += 1
                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    entityUrn=urn,
                    changeType=ChangeTypeClass.UPSERT,
                    aspectName="status",
                    aspect=Status(removed=True),
                )
                yield MetadataWorkUnit(id=f"soft-delete-{urn}", mcp=mcp)

    def close(self):
        self.prepare_for_commit()
        self.client.session.close()

    def get_report(self) -> RedashSourceReport:
        return self.report

    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        if (
                job_id == self.get_default_ingestion_job_id()
                and self.is_stateful_ingestion_configured()
                and self.config.stateful_ingestion
                and self.config.stateful_ingestion.remove_stale_metadata
        ):
            return True

        return False

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        assert self.ctx.pipeline_name is not None
        if job_id == self.get_default_ingestion_job_id():
            return Checkpoint(
                job_name=job_id,
                pipeline_name=self.ctx.pipeline_name,
                platform_instance_id=self.get_platform_instance_id(),
                run_id=self.ctx.run_id,
                config=self.config,
                state=RedashCheckpointState(),
            )
        return None

    def get_platform_instance_id(self) -> str:
        assert self.config.platform_instance is not None
        return self.config.platform_instance

    @abstractmethod
    def get_default_ingestion_job_id(self) -> JobId:
        raise NotImplementedError("Sub-classes must implement this method.")


class IBRedashDatasetSource(IBRedashSource):
    containers_cache = []

    @property
    @abstractmethod
    def platform(self):
        raise NotImplementedError("Sub-classes must define this variable.")

    @property
    @abstractmethod
    def parent_subtypes(self) -> List[str]:
        raise NotImplementedError("Sub-classes must define this variable.")

    @property
    @abstractmethod
    def object_subtype(self):
        raise NotImplementedError("Sub-classes must define this variable.")

    def __init__(self, config: IBRedashSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config: IBRedashSourceConfig = config

    def fetch_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        logger.error("START fetch_workunits")
        json_data = pd.read_json(json.dumps(self.query_get(self.config.query_id)))
        json_data_grouped = json_data.groupby(["locationCode", "parent1", "parent2", "parent3", "objectName"],
                                              dropna=False)
        result = json_data_grouped.apply(
            lambda fields_by_object: self.fetch_object_workunits(fields_by_object)
        )
        logger.error(f"RESULT TYPE:{type(result)}")
        logger.error("START END")
        return result

    def fetch_object_workunits(self, fields_by_object: pd.DataFrame) -> Iterable[MetadataWorkUnit]:
        logger.error("START fetch_object_workunits")
        object_sample = fields_by_object.iloc[0]
        object_name = object_sample.objectName

        dataset_path = [object_sample.locationCode, object_sample.parent1, object_sample.parent2, object_sample.parent3,
                        object_name]

        logger.error("BEFORE fetch_containers_workunits")
        yield from self.fetch_containers_workunits(*dataset_path)
        logger.error("AFTER fetch_containers_workunits")

        properties = DatasetPropertiesClass(
            name=object_name,
            description=object_sample.description,
            qualifiedName=build_dataset_qualified_name(*dataset_path),
        )

        subtypes = SubTypesClass(typeNames=[self.object_subtype])

        browse_paths = BrowsePathsClass([build_dataset_browse_path(*dataset_path)])

        schema = SchemaMetadataClass(
            schemaName=self.platform,
            version=1,
            hash="",
            platform=builder.make_data_platform_urn(self.platform),
            platformSchema=KafkaSchemaClass.construct_with_defaults(),
            fields=fields_by_object.dropna(subset="fieldName").apply(lambda field: self.map_column(field),
                                                                     axis=1).values.tolist(),
        )
        owners = [
            builder.make_group_urn(owner.strip()) for owner in object_sample.owners.split(",")
        ]
        ownership = builder.make_ownership_aspect_from_urn_list(
            owners, OwnershipSourceTypeClass.SERVICE, OwnershipTypeClass.TECHNICAL_OWNER
        )
        aspects = [properties, subtypes, browse_paths, schema, ownership]
        snapshot = DatasetSnapshot(
            urn=build_dataset_urn(self.platform, *dataset_path),
            aspects=aspects,
        )
        mce = MetadataChangeEvent(proposedSnapshot=snapshot)
        yield MetadataWorkUnit(properties.qualifiedName, mce=mce)

    def fetch_containers_workunits(self, location_code: str, *dataset_parents: str) -> Iterable[MetadataWorkUnit]:
        dataset_parents = [location_code.lower()] + list(dataset_parents)
        parent = None
        for i in range(1, len(dataset_parents)):
            parent = yield from self.fetch_container_workunits(parent, self.parent_subtypes[i], dataset_parents[:i])

    def fetch_container_workunits(self, parent: Optional[MetadataWorkUnit], container_subtype: str, *path) \
            -> Iterable[MetadataWorkUnit]:
        qualified_name = build_path_with_separator('.', *path)
        container_urn = builder.make_container_urn(qualified_name)
        if container_urn in self.containers_cache:
            return

        self.containers_cache.append(container_urn)

        yield self.build_container_workunit_with_aspect(container_urn,
                                                        aspect=ContainerProperties(
                                                            name=path[-1],
                                                            qualifiedName=qualified_name))

        yield self.build_container_workunit_with_aspect(container_urn, SubTypesClass(typeNames=[container_subtype]))

        if parent is not None:
            yield self.build_container_workunit_with_aspect(container_urn,
                                                            ContainerClass(container=parent.metadata.entityUrn))

    @staticmethod
    def build_container_workunit_with_aspect(urn: str, aspect):
        mcp = MetadataChangeProposalWrapper(
            entityType="container",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=f"{urn}",
            aspect=aspect,
        )
        return MetadataWorkUnit(id=f"{urn}-{type(aspect).__name__}", mcp=mcp)

    @staticmethod
    def map_column(field) -> SchemaFieldClass:
        data_type = field.fieldType
        return SchemaFieldClass(
            fieldPath=field.fieldName,
            description=field.valueSet,
            type=SchemaFieldDataTypeClass(type=get_type_class(data_type)),
            nativeDataType=data_type,
            nullable=bool(field.nullable),
        )

    @abstractmethod
    def get_default_ingestion_job_id(self) -> JobId:
        pass


def get_type_class(type_str: str):
    type_str = type_str.lower() if type_str is not None else "undefined"
    type_class: Union[
        "StringTypeClass",
        "BooleanTypeClass",
        "NumberTypeClass",
        "BytesTypeClass",
        "DateTypeClass",
        "NullTypeClass",
    ]
    if type_str in [
        "string",
        "char",
        "nchar",
        "varchar",
        "varchar(n)",
        "varchar(max)",
        "nvarchar",
        "nvarchar(max)",
        "text",
    ]:
        return StringTypeClass()
    elif type_str in ["bit", "boolean"]:
        return BooleanTypeClass()
    elif type_str in [
        "integer",
        "int",
        "tinyint",
        "smallint",
        "bigint",
        "float",
        "real",
        "decimal",
        "numeric",
        "money",
    ]:
        return NumberTypeClass()
    elif type_str in ["object", "binary", "varbinary", "varbinary(max)"]:
        return BytesTypeClass()
    elif type_str in ["date", "smalldatetime", "datetime", "datetime2", "timestamp"]:
        return DateTypeClass()
    elif type_str in ["array"]:
        return ArrayTypeClass()
    else:
        return NullTypeClass()


def build_dataset_urn(platform: str, location_code: str, *path: str):
    return builder.make_dataset_urn(
        platform.lower(), build_dataset_path_with_separator('.', location_code, *path), "PROD"
    )


def build_dataset_qualified_name(location_code: str, *path: str):
    return build_dataset_path_with_separator('.', location_code, *path)


def build_dataset_browse_path(location_code: str, *path: str):
    return f"/prod/{build_dataset_path_with_separator('/', location_code, *path)}"


def build_dataset_path_with_separator(separator: str, location_code: str, *path: str):
    return build_path_with_separator(separator, location_code.lower(), *path)


def build_path_with_separator(separator: str, *path: str):
    return f"{separator.join(filter(lambda e: e is not None, path))}"
