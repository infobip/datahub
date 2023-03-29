import json
import logging
from typing import List

import pandas as pd
import pyodbc
from krbcontext.context import krbContext
from pandas import DataFrame
from redash_toolbelt import Redash
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from datahub.ingestion.source.ib.dao.ib_dao_config import (
    IBMSSQLDatabaseConfigEntry,
    IBMSSQLSourceConfig,
    IBRedashSourceConfig,
)

logger = logging.getLogger(__name__)


class IBDao:
    @staticmethod
    def load_redash_data(config: IBRedashSourceConfig) -> DataFrame:
        client = Redash(config.connect_uri.strip("/"), config.api_key)
        client.session.headers.update(
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
        client.session.mount("http://", adapter)
        client.session.mount("https://", adapter)

        url = f"//api/queries/{config.query_id}/results"

        try:
            response = client._post(url).json()["query_result"]["data"]["rows"]
            return pd.read_json(json.dumps(response))
        finally:
            client.session.close()
            adapter.close()

    @staticmethod
    def load_mssql_data(config: IBMSSQLSourceConfig) -> DataFrame:
        dataframes: List[DataFrame] = list()
        for database in config.databases:
            IBDao._process_single_database(database, dataframes)

        if len(dataframes) == 1:
            return dataframes[0]
        else:
            return pd.concat(dataframes)

    @staticmethod
    def _process_single_database(
        database: IBMSSQLDatabaseConfigEntry, dataframes: List[DataFrame]
    ):
        with krbContext(
            using_keytab=True,
            principal=database.username,
            keytab_file=database.keytab_file,
        ):
            with pyodbc.connect(database.connection_string) as conn:
                for file_name in database.sql_files:
                    query: str
                    with open(file_name) as file:
                        query = file.read()
                    dataframes.append(pd.read_sql_query(query, conn))
