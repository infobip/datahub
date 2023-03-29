from pandas import DataFrame

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.source.ib.dao.ib_dao import IBDao
from datahub.ingestion.source.ib.dao.ib_dao_config import IBMSSQLSourceConfig
from datahub.ingestion.source.ib.dataset.ib_dataset import IBDatasetSource
from datahub.ingestion.source.state.stateful_ingestion_base import JobId


class IBMSSQLViewsSourceConfig(IBMSSQLSourceConfig):
    pass


@platform_name("IBMSSQL")
@config_class(IBMSSQLViewsSourceConfig)
class IBMSSQLViewsSource(IBDatasetSource):
    platform = "mssql"
    object_subtype = "View"

    @classmethod
    def create(cls, config_dict, ctx):
        config = IBMSSQLViewsSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def __init__(self, config: IBMSSQLViewsSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: IBMSSQLViewsSourceConfig = config

    def get_default_ingestion_job_id_prefix(self) -> JobId:
        return JobId("ingest_mssql_views_from_redash_source_")

    def load_data(self) -> DataFrame:
        return IBDao.load_mssql_data(self.config)
