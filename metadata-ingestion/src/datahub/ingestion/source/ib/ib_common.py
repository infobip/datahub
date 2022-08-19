import sys
import math
from abc import abstractmethod
from typing import Union, Iterable
import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.source import Source, SourceReport
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from redash_toolbelt import Redash
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    NumberTypeClass,
    StringTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    ArrayTypeClass,
)
from pydantic.fields import Field
from urllib3 import Retry
from requests.adapters import HTTPAdapter


class IBRedashSourceConfig(ConfigModel):
    connect_uri: str = Field(
        default="http://localhost:5000", description="Redash base URL."
    )
    api_key: str = Field(default="REDASH_API_KEY", description="Redash user API key.")
    query_id: str = Field(
        default="QUERY_ID",
        description="Target redash query",
    )
    api_page_limit: int = Field(
        default=sys.maxsize,
        description="Limit on number of pages queried for ingesting dashboards and charts API "
                    "during pagination. ",
    )


class IBRedashSource(Source):
    batch_size = 1000
    config: IBRedashSourceConfig
    client: Redash
    report: SourceReport

    def __init__(self, config: IBRedashSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config: IBRedashSourceConfig = config
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
        config = IBRedashSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def query_get(self, query_id) -> str:
        url = f"//api/queries/{query_id}/results"
        return self.client._post(url).json()["query_result"]["data"]["rows"]

    @abstractmethod
    def get_workunits(self) -> Iterable[WorkUnit]:
        pass

    def close(self):
        self.client.session.close()

    # todo implement?
    def get_report(self) -> SourceReport:
        pass


def get_type_class(type_str: str):
    type_str = type_str.lower() if type_str is not None else "undefined"
    type_class: Union[
        "StringTypeClass", "BooleanTypeClass", "NumberTypeClass", "BytesTypeClass", "DateTypeClass", "NullTypeClass"]
    if type_str in ["string",
                    "char", "nchar",
                    "varchar", "varchar(n)", "varchar(max)",
                    "nvarchar", "nvarchar(max)",
                    "text"]:
        return StringTypeClass()
    elif type_str in ["bit", "boolean"]:
        return BooleanTypeClass()
    elif type_str in ["integer", "int", "tinyint", "smallint", "bigint",
                      "float", "real", "decimal", "numeric", "money"]:
        return NumberTypeClass()
    elif type_str in ["object", "binary", "varbinary", "varbinary(max)"]:
        return BytesTypeClass()
    elif type_str in ["date", "smalldatetime", "datetime", "datetime2", "timestamp"]:
        return DateTypeClass()
    elif type_str in ["array"]:
        return ArrayTypeClass()
    else:
        return NullTypeClass()


def build_dataset_urn(platform: str, name: str, *parents: str):
    return builder.make_dataset_urn(platform.lower(), f"{'.'.join(parents)}.{name}", "PROD")
