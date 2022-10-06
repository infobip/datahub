from datahub.ingestion.api.decorators import config_class, platform_name

from datahub.ingestion.source.ib.ib_common import *


class IBMSSQLViewsSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBMSSQL")
@config_class(IBMSSQLViewsSourceConfig)
class IBMSSQLViewsSource(IBRedashDatasetSource):
    parent_subtypes = ["DataCenter", "Server", "Database", "Schema"]
    object_subtype = "View"
    platform = "mssql"

    def __init__(self, config: IBMSSQLViewsSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: IBMSSQLViewsSourceConfig = config

    def get_default_ingestion_job_id(self) -> JobId:
        return JobId("ingest_mssql_views_from_redash_source")
