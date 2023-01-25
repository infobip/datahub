from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.ib.dataset.ib_dataset import IBRedashDatasetSource
from datahub.ingestion.source.ib.ib_common import IBRedashSourceConfig


class IBElasticsearchSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBKafka")
@config_class(IBElasticsearchSourceConfig)
class IBElasticsearchSource(IBRedashDatasetSource):
    platform = "elasticsearch"
    object_subtype = "Index"

    def __init__(self, config: IBElasticsearchSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: IBElasticsearchSourceConfig = config

    def get_default_ingestion_job_id_prefix(self) -> JobId:
        return JobId("ingest_elasticsearch_from_redash_source_")
