import json

import pandas as pd
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DatasetPropertiesClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    KafkaSchemaClass,
    SchemaMetadataClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
)

from src.datahub.ingestion.source.ib.ib_common import *


class IBOwnersSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBKafka")
@config_class(IBOwnersSourceConfig)
class IBOwnersSource(IBRedashSource):
    platform = "kafka"

    # 6v8YQZxGdaGZM6z9pMmrqV7c0v8r22QLejHB736q

    def __init__(self, config: IBOwnersSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        return pd.read_json(json.dumps(self.query_get(self.config.query_id))).groupby(
            ["dc", "cluster", "topic"], dropna=False).apply(lambda key, rows: self.build_workunit(key, rows), axis=1)

    def get_workunit(self, key: tuple, rows: pd.DataFrame):
        first = rows.iloc[0]
        topic_name = first.topic

        parents = [
            first.dc,
            first.cluster,
        ]
        dot_joined_parents = '.'.join(parents)

        properties = DatasetPropertiesClass(
            name=topic_name,
            description=first.description,
            qualifiedName=f"{dot_joined_parents}.{topic_name}"
        )

        browse_paths = BrowsePathsClass([f"/prod/{self.platform}/{'/'.join(parents)}/{topic_name}"])

        schema = SchemaMetadataClass(
            schemaName=self.platform,
            version=1,
            hash="",
            platform=f"urn:li:dataPlatform:{self.platform}",
            platformSchema=KafkaSchemaClass.construct_with_defaults(),
            fields=[] if len(rows.index) == 1 and first.fieldName is None and first.fieldType is None else
            [rows.apply(lambda column: self.map_column(column), axis=1)],
        )
        owners = [builder.make_group_urn(owner.strip()) for owner in first.owners.split(",")]
        ownership = builder.make_ownership_aspect_from_urn_list(owners,
                                                                OwnershipSourceTypeClass.SERVICE,
                                                                OwnershipTypeClass.TECHNICAL_OWNER)
        aspects = [properties, browse_paths, schema, ownership]
        snapshot = DatasetSnapshot(
            urn=f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dot_joined_parents}.{topic_name},PROD)",
            aspects=aspects,
        )
        mce = MetadataChangeEvent(proposedSnapshot=snapshot)
        return MetadataWorkUnit(properties.qualifiedName, mce=mce)

    @staticmethod
    def map_column(row):
        data_type = row.fieldType
        return SchemaFieldClass(
            fieldPath=row.fieldName,
            description=row.description,
            type=SchemaFieldDataTypeClass(type=get_type_class(data_type)),
            nativeDataType=data_type,
            nullable=bool(row.nullable),
        )
