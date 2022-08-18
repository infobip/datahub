import json
import math
import sys
from typing import Iterable, Union

import datahub.emitter.mce_builder as builder
import pandas as pd
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
)
from pydantic.fields import Field
from redash_toolbelt import Redash
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class IBTechnicalOwnersSourceConfig(ConfigModel):
    connect_uri: str = Field(
        default="http://localhost:5000", description="Redash base URL."
    )
    api_key: str = Field(default="REDASH_API_KEY", description="Redash user API key.")
    lineage_query_id: str = Field(
        default="LINEAGES_QUERY_ID",
        description="Target redash query that contains all the " "lineages data",
    )
    api_page_limit: int = Field(
        default=sys.maxsize,
        description="Limit on number of pages queried for ingesting dashboards and charts API "
                    "during pagination. ",
    )


@platform_name("TechnicalOwners")
@config_class(IBTechnicalOwnersSourceConfig)
class IBTechnicalOwnersSource(Source):
    batch_size = 1000
    config: IBTechnicalOwnersSourceConfig
    client: Redash
    report: SourceReport

    def __init__(self, config: IBTechnicalOwnersSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config: IBTechnicalOwnersSourceConfig = config
        self.report: SourceReport = SourceReport()

        self.config.connect_uri = self.config.connect_uri.strip("/")
        self.client = Redash(self.config.connect_uri, self.config.api_key)
        self.client.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        # Handling retry and backoff
        retries = 3
        backoff_factor = 10
        status_forcelist = (500, 503, 502, 504)
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.client.session.mount("http://", adapter)
        self.client.session.mount("https://", adapter)

        self.api_page_limit = self.config.api_page_limit or math.inf

    @classmethod
    def create(cls, config_dict, ctx):
        config = IBTechnicalOwnersSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        ownerships = pd.read_json(json.dumps(self.get_owners(self.config.lineage_query_id)))
        return ownerships.apply(lambda ownership: self.build_workunit(ownership), axis=1).tolist()

    @staticmethod
    def build_workunit(ownership):
        dataset_urn = builder.make_dataset_urn("kafka",
                                               f"{ownership.dc.lower()}.{ownership.cluster}.{ownership.topic}")
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

    def get_owners(self, query_id) -> str:
        url = f"api/queries/{query_id}/results"
        return self.client._post(url).json()["query_result"]["data"]["rows"]

    @staticmethod
    def build_dataset_urn(
            dataset_type, location_code, db_name, schema_name, table_name
    ) -> str:
        if dataset_type.lower() == "kafka":
            return builder.make_dataset_urn(
                "kafka", f"{location_code.lower()}.{db_name}.{table_name}", "PROD"
            )
        else:
            return builder.make_dataset_urn(
                "mssql",
                f"{location_code.lower()}.{db_name}.{schema_name}.{table_name}",
                "PROD",
            )

    def get_report(self):
        return self.report

    def close(self):
        self.client.close()
