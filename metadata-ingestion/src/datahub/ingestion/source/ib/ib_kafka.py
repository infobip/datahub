import json
from typing import Iterable

import pandas as pd
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.schema_classes import DatasetLineageTypeClass

from src.datahub.ingestion.source.ib.ib_common import *


class IBOwnersSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBKafka")
@config_class(IBOwnersSourceConfig)
class IBOwnersSource(IBRedashSource):
    # 6v8YQZxGdaGZM6z9pMmrqV7c0v8r22QLejHB736q

    def __init__(self, config: IBOwnersSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        grouped_per_topic = pd.read_json(json.dumps(self.query_get(self.config.query_id))).groupby(
            ["dc", "cluster", "topic"], dropna=False)
        lineages_grouped

