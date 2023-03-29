from pandas import DataFrame

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.source.ib.dao.ib_dao import IBDao
from datahub.ingestion.source.ib.dao.ib_dao_config import IBRedashSourceConfig
from datahub.ingestion.source.ib.dataset.ib_dataset import IBDatasetSource


class IBKafkaSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBKafka")
@config_class(IBKafkaSourceConfig)
class IBKafkaSource(IBDatasetSource):
    platform = "kafka"
    object_subtype = "Kafka Topic"

    @classmethod
    def create(cls, config_dict, ctx):
        config = IBKafkaSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def __init__(self, config: IBKafkaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: IBKafkaSourceConfig = config

    def get_default_ingestion_job_id_prefix(self) -> JobId:
        return JobId("ingest_kafka_from_redash_source_")

    def load_data(self) -> DataFrame:
        return IBDao.load_redash_data(self.config)
