from typing import List

from pydantic import BaseModel
from pydantic.fields import Field

from datahub.ingestion.source.ib.ib_stateful_ingestion_source import IBSourceConfig


class IBRedashSourceConfig(IBSourceConfig):
    connect_uri: str = Field(
        default="http://localhost:5000", description="Redash base URL."
    )
    api_key: str = Field(default="REDASH_API_KEY", description="Redash user API key.")
    query_id: str = Field(
        default="QUERY_ID",
        description="Target redash query",
    )


class IBMSSQLDatabaseConfigEntry(BaseModel):
    username: str = Field(default="", description="Kerberos user")
    keytab_file: str = Field(
        default="/etc/krb.keytab", description="Kerberos keytab file"
    )
    connection_string: str = Field(
        default="", description="MSSQL database connection string"
    )
    sql_files: List[str] = Field(
        default="",
        description="Sql files to execute against specified database",
    )


class IBMSSQLSourceConfig(IBSourceConfig):
    databases: List[IBMSSQLDatabaseConfigEntry] = Field(
        default=[], description="Databases list"
    )
