from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.ib.dataset.ib_dataset import IBRedashDatasetSource
from datahub.ingestion.source.ib.ib_common import IBRedashSourceConfig


class IBPostgreSQLSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBPostgreSQL")
@config_class(IBPostgreSQLSourceConfig)
class IBPostgreSQLSource(IBRedashDatasetSource):
    platform = "postgres"
    object_subtype = "Table"

    def __init__(self, config: IBPostgreSQLSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: IBPostgreSQLSourceConfig = config

    def get_default_ingestion_job_id_prefix(self) -> JobId:
        return JobId("ingest_postgresql_from_redash_source_")
