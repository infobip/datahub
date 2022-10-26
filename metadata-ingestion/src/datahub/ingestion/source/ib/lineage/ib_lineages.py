import json
import logging
from typing import Iterable, Union

import pandas as pd

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.ingestion.source.ib.ib_common import IBRedashSource, IBRedashSourceConfig
from datahub.ingestion.source.ib.utils.dataset_utils import (
    DatasetUtils,
    IBGenericPathElements,
)
from datahub.ingestion.source.state.stateful_ingestion_base import JobId
from datahub.metadata.schema_classes import DatasetLineageTypeClass

logger = logging.getLogger(__name__)


class IBLineagesSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBLineages")
@config_class(IBLineagesSourceConfig)
class IBLineagesSource(IBRedashSource):
    def __init__(self, config: IBLineagesSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

    def fetch_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        lineages_grouped = pd.read_json(
            json.dumps(self.query_get(self.config.query_id))
        ).groupby(
            [
                "dstType",
                "dstLocationCode",
                "dstParent1",
                "dstParent2",
                "dstParent3",
                "dstObjectName",
            ],
            dropna=False,
        )

        for key_tuple, lineages in lineages_grouped:
            first = lineages.iloc[0]
            dst_platform = first.dstType.lower()
            dst_dataset_path = DatasetUtils.map_path(
                dst_platform,
                None,
                IBGenericPathElements(
                    location_code=first.dstLocationCode,
                    parent1=first.dstParent1,
                    parent2=first.dstParent2,
                    parent3=first.dstParent3,
                    object_name=first.dstObjectName,
                ),
            )
            dst_dataset_urn = DatasetUtils.build_dataset_urn(
                dst_platform, *dst_dataset_path
            )

            src_urns = []
            # TODO for to map()/transform()
            for index, row in lineages.iterrows():
                src_platform = row.srcType.lower()
                src_dataset_path = DatasetUtils.map_path(
                    src_platform,
                    None,
                    IBGenericPathElements(
                        location_code=row.srcLocationCode,
                        parent1=row.srcParent1,
                        parent2=row.srcParent2,
                        parent3=row.srcParent3,
                        object_name=row.srcObjectName,
                    ),
                )
                src_urns.append(
                    DatasetUtils.build_dataset_urn(src_platform, *src_dataset_path)
                )

            yield MetadataWorkUnit(
                dst_dataset_urn,
                mce=builder.make_lineage_mce(
                    src_urns, dst_dataset_urn, DatasetLineageTypeClass.COPY
                ),
            )

    def get_default_ingestion_job_id_prefix(self) -> JobId:
        return JobId("ingest_lineages_from_redash_source_")
