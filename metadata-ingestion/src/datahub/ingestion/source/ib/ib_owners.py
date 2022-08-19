import json
from typing import Iterable

import pandas as pd
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
)

from src.datahub.ingestion.source.ib.ib_common import *


class IBOwnersSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBOwners")
@config_class(IBOwnersSourceConfig)
class IBOwnersSource(IBRedashSource):

    def __init__(self, config: IBOwnersSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        ownerships = pd.read_json(json.dumps(self.query_get(self.config.query_id)))
        return ownerships.apply(lambda ownership: self.build_workunit(ownership), axis=1)

    @staticmethod
    def build_workunit(ownership):
        # todo add platform to source query and remove hardcode
        dataset_urn = build_dataset_urn("kafka", ownership.topic, ownership.dc.lower(), ownership.cluster)
        owners = [builder.make_group_urn(owner.strip()) for owner in ownership.owners.split(",")]
        ownership_aspect = builder.make_ownership_aspect_from_urn_list(owners,
                                                                       OwnershipSourceTypeClass.SERVICE,
                                                                       OwnershipTypeClass.TECHNICAL_OWNER)
        return MetadataWorkUnit(
            dataset_urn,
            mce=MetadataChangeEventClass(
                proposedSnapshot=DatasetSnapshotClass(
                    urn=dataset_urn,
                    aspects=[ownership_aspect],
                )
            )
        )
