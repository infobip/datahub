from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.ib.ib_common import (
    IBRedashDatasetSource,
    IBRedashSourceConfig,
)


class IBKafkaSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBKafka")
@config_class(IBKafkaSourceConfig)
class IBKafkaSource(IBRedashDatasetSource):
    parent_subtypes = ["DataCenter", "Kafka Cluster"]
    object_subtype = "Kafka Topic"
    platform = "kafka"

    def __init__(self, config: IBKafkaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: IBKafkaSourceConfig = config

    def get_default_ingestion_job_id_prefix(self) -> JobId:
        return JobId("ingest_kafka_from_redash_source_")
