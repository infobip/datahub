import json
import logging
import math
import sys
import re
from abc import abstractmethod
from typing import Iterable, List, Optional, Union, cast

import pandas as pd
from pydantic.fields import Field
from redash_toolbelt import Redash
from requests.adapters import HTTPAdapter
from urllib3 import Retry

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.redash_state import RedashCheckpointState
from datahub.ingestion.source.state.stateful_ingestion_base import (
    JobId,
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    DataPlatformInstance,
    Status,
)
from datahub.metadata.com.linkedin.pegasus2avro.container import ContainerProperties
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BrowsePathsClass,
    BytesTypeClass,
    ChangeTypeClass,
    ContainerClass,
    DatasetPropertiesClass,
    DateTypeClass,
    KafkaSchemaClass,
    NullTypeClass,
    NumberTypeClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
)

logger = logging.getLogger(__name__)
ib_location_platform = "infobip-location"


class IBRedashSourceStatefulIngestionConfig(StatefulIngestionConfig):
    remove_stale_metadata: bool = True


class RedashSourceReport(StatefulIngestionReport):
    events_skipped: int = 0
    events_deleted: int = 0


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
                self.report.report_workunit(wu)
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

        last_checkpoint = self.get_last_checkpoint(
            self.get_default_ingestion_job_id(), RedashCheckpointState
        )
        last_checkpoint_state = (
            cast(RedashCheckpointState, last_checkpoint.state)
            if last_checkpoint is not None
            else None
        )

        for wu in self.fetch_workunits():
            if type(wu) is not MetadataWorkUnit:
                yield wu
                continue

            if cur_checkpoint_state is not None:
                if type(wu.metadata) is MetadataChangeEvent:
                    urn = wu.metadata.proposedSnapshot.urn
                    wu.metadata.proposedSnapshot.aspects.append(
                        StatusClass(removed=False)
                    )
                elif type(wu.metadata) is MetadataChangeProposalWrapper:
                    urn = wu.metadata.entityUrn
                else:
                    raise TypeError(f"Unknown metadata type {type(wu.metadata)}")

                cur_checkpoint_state.add_workunit(urn, wu)

                # Emitting workuntis not presented in last state
                if (
                        last_checkpoint_state is None
                        or not last_checkpoint_state.has_workunit(urn, wu)
                ):
                    self.report.report_workunit(wu)
                    yield wu
                    continue
                else:
                    self.report.events_skipped += 1
            else:
                self.report.report_workunit(wu)
                yield wu

        if (
                self.config.stateful_ingestion
                and self.config.stateful_ingestion.remove_stale_metadata
                and last_checkpoint_state is not None
                and cur_checkpoint_state is not None
        ):
            # Deleting workunits not presented in current state
            for urn in last_checkpoint_state.get_urns_not_in(cur_checkpoint_state):
                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    entityUrn=urn,
                    changeType=ChangeTypeClass.UPSERT,
                    aspectName="status",
                    aspect=Status(removed=True),
                )
                self.report.workunits_deleted += 1
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


class IBPathElementInfo:
    subtype: str
    is_location: bool

    def __init__(self, subtype: str, is_location: bool = False):
        self.subtype: str = subtype
        self.is_location: bool = is_location


class IBRedashDatasetSource(IBRedashSource):
    containers_cache = []

    @property
    @abstractmethod
    def platform(self):
        raise NotImplementedError("Sub-classes must define this variable.")

    @property
    @abstractmethod
    def path_info(self) -> List[IBPathElementInfo]:
        raise NotImplementedError("Sub-classes must define this variable.")

    def __init__(self, config: IBRedashSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config: IBRedashSourceConfig = config

    def fetch_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        json_data = pd.read_json(json.dumps(self.query_get(self.config.query_id)))

        for i, row in json_data.iterrows():
            yield from self.fetch_object_workunits(row)

    def fetch_object_workunits(self, row: pd.DataFrame) -> Iterable[MetadataWorkUnit]:
        object_name = row.objectName

        dataset_path = self.normalize_dataset_path([
            row.locationCode,
            row.parent1,
            row.parent2,
            row.parent3,
            object_name,
        ])

        properties = DatasetPropertiesClass(
            name=object_name,
            description=row.description,
            qualifiedName=build_dataset_qualified_name(*dataset_path),
        )

        browse_paths = BrowsePathsClass([build_dataset_browse_path(*dataset_path)])

        columns = (
            list(map(lambda col: self.map_column(col), row.columns.split('|;|')))
            if row.columns is not None
            else []
        )
        schema = SchemaMetadataClass(
            schemaName=self.platform,
            version=1,
            hash="",
            platform=builder.make_data_platform_urn(self.platform),
            platformSchema=KafkaSchemaClass.construct_with_defaults(),
            fields=columns,
        )

        owners = []
        if row.owners is not None:
            owners = [
                builder.make_group_urn(owner.strip())
                for owner in row.owners.split(",")
            ]

        ownership = builder.make_ownership_aspect_from_urn_list(
            owners, OwnershipSourceTypeClass.SERVICE, OwnershipTypeClass.TECHNICAL_OWNER
        )
        aspects = [properties, browse_paths, schema, ownership]
        snapshot = DatasetSnapshot(
            urn=build_dataset_urn(self.platform, *dataset_path),
            aspects=aspects,
        )
        mce = MetadataChangeEvent(proposedSnapshot=snapshot)
        yield MetadataWorkUnit(properties.qualifiedName, mce=mce)

        yield MetadataWorkUnit(
            id=f"{properties.qualifiedName}-subtype",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=snapshot.urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=[self.path_info[-1].subtype]),
            ),
        )

        container_parent_path = None
        for i in range(1, len(dataset_path)):
            container_path = dataset_path[:i]
            if pd.isna(container_path[-1]):
                break
            yield from self.fetch_container_workunits(
                container_path, self.path_info[i - 1], container_parent_path
            )
            container_parent_path = container_path

        yield MetadataWorkUnit(
            id=f"{properties.qualifiedName}-container",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=snapshot.urn,
                aspectName="container",
                aspect=ContainerClass(
                    container=build_container_urn(*container_parent_path)
                ),
            ),
        )

    def fetch_container_workunits(
            self,
            path: List[str],
            container_info: IBPathElementInfo,
            parent_path: Optional[List[str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        qualified_name = build_dataset_qualified_name(*path)
        container_urn = builder.make_container_urn(qualified_name)
        if container_urn in self.containers_cache:
            return

        self.containers_cache.append(container_urn)

        yield self.build_container_workunit_with_aspect(
            container_urn,
            aspect=ContainerProperties(name=path[-1], qualifiedName=qualified_name),
        )

        yield self.build_container_workunit_with_aspect(
            container_urn, SubTypesClass(typeNames=[container_info.subtype])
        )

        yield self.build_container_workunit_with_aspect(
            container_urn,
            aspect=DataPlatformInstance(
                platform=builder.make_data_platform_urn(ib_location_platform) if container_info.is_location
                else builder.make_data_platform_urn(self.platform),
            ),
        )

        if parent_path is not None:
            yield self.build_container_workunit_with_aspect(
                container_urn,
                ContainerClass(container=build_container_urn(*parent_path)),
            )

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
        parts = field.split("|:|")
        data_type = parts[1]
        return SchemaFieldClass(
            fieldPath=parts[0],
            description=parts[3],
            type=SchemaFieldDataTypeClass(type=get_type_class(data_type)),
            nativeDataType=data_type,
            nullable=bool(parts[2]),
        )

    def normalize_dataset_path(self, dataset_path: List[str]):
        return list(map(lambda i_el: i_el[1].lower() if self.path_info[i_el[0]].is_location else i_el[1],
                        enumerate(filter(lambda e: not pd.isna(e), dataset_path))))

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


def build_dataset_urn(platform: str, *path: str):
    return builder.make_dataset_urn(
        platform.lower(),
        build_dataset_path_with_separator(".", *path),
        "PROD",
    )


def build_container_urn(*path: str):
    return builder.make_container_urn(build_dataset_qualified_name(*path))


def build_dataset_qualified_name(*path: str):
    return build_dataset_path_with_separator(".", *path)


def build_dataset_browse_path(*path: str):
    return f"/prod/{build_dataset_path_with_separator('/', *path)}"


def build_dataset_path_with_separator(separator: str, *path: str):
    return build_str_path_with_separator(separator, *path)


def build_str_path_with_separator(separator: str, *path: str):
    replace_chars_regex = re.compile('[/\\\\&?*=]')
    return separator.join(map(lambda p: replace_chars_regex.sub('-', p), path))

