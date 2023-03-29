import json
import logging
import re
from typing import Iterable, Union

import pandas as pd
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.ingestion.source.ib.ib_common import IBRedashSource, IBRedashSourceConfig
from datahub.ingestion.source.state.stateful_ingestion_base import JobId
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ContainerPropertiesClass
)

logger = logging.getLogger(__name__)


class IBExtendedPropertiesSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBExtendedProperties")
@config_class(IBExtendedPropertiesSourceConfig)
class IBExtendedPropertiesSource(IBRedashSource):
    def __init__(self, config: IBExtendedPropertiesSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

    def fetch_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        extended_properties_grouped = pd.read_json(
            json.dumps(self.query_get(self.config.query_id))
        ).groupby(
            [
                "locationCode",
                "parent1",
                "parent2",
            ],
            dropna=False,
        )

        for key_tuple, extended_properties in extended_properties_grouped:
            row = extended_properties.iloc[0]
            urn = build_container_urn(row.locationCode, row.parent1, row.parent2)

            yield MetadataWorkUnit(
                id=f"{urn}-extended-properties",
                mcp=MetadataChangeProposalWrapper(
                    entityType="container",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=urn,
                    aspectName="upstreamLineage",
                    aspect=ContainerPropertiesClass(name="backup_info", customProperties=dict(
                        zip(extended_properties["property_key"], extended_properties["property_value"]))),
                ),
            )

    def get_default_ingestion_job_id_prefix(self) -> JobId:
        return JobId("ingest_extended_properties_from_redash_source_")


# TODO Refac after DI-1665 is done
def build_container_urn(*path):
    replace_chars_regex = re.compile("[/\\\\&?*=]")
    return "urn:li:container:" + ".".join(
        map(lambda p: replace_chars_regex.sub("-", p.value), path)
    )
