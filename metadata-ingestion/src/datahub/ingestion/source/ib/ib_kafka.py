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

from datahub.ingestion.source.ib.ib_common import *


class IBKafkaSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBKafka")
@config_class(IBKafkaSourceConfig)
class IBKafkaSource(IBRedashSource):
    platform = "kafka"

    def __init__(self, config: IBKafkaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        json_data = pd.read_json(json.dumps(self.query_get(self.config.query_id)))
        json_data_grouped = json_data.groupby(["dc", "cluster", "topic"], dropna=False)
        return json_data_grouped.apply(lambda fields_by_topic: self.get_workunit(fields_by_topic))

    def get_workunit(self, fields_by_topic: pd.DataFrame):
        first = fields_by_topic.iloc[0]
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
            fields=fields_by_topic.dropna(subset=['fieldName', 'fieldType'], how='all').apply(
                lambda field: self.map_column(field), axis=1),
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
    def map_column(field) -> SchemaFieldClass:
        data_type = field.fieldType
        return SchemaFieldClass(
            fieldPath=field.fieldName,
            description=field.valueSet,
            type=SchemaFieldDataTypeClass(type=get_type_class(data_type)),
            nativeDataType=data_type,
            nullable=bool(field.nullable),
        )
