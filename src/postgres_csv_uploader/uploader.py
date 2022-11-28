import re
from io import StringIO
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2 as ps
import psycopg2.sql as sql
from psycopg2.extensions import connection


class PostgresCSVUploader:
    def __init__(self, conn: connection):
        self.conn = conn
        self.data = None
        self.buffer = StringIO()
        # Pandas & Numpy types referenced from
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.infer_objects.html
        # and PostgreSQL types matched from https://www.postgresql.org/docs/9.1/datatype.html
        self.pg_2_sql_map = {
            "uint8": "SMALLINT",
            "uint16": "SMALLINT",
            "uint32": "INTEGER",
            "uint64": "BIGINT",
            "int": "INTEGER",
            "int8": "SMALLINT",
            "int16": "INTEGER",
            "int32": "INTEGER",
            "int64": "BIGINT",
            "complex64": "VARCHAR",
            "complex128": "VARCHAR",
            "str": "VARCHAR",
            "object": "VARCHAR",
            "category": "VARCHAR",
            "Decimal": "NUMERIC",
            "float": "FLOAT",
            "float16": "FLOAT",
            "float32": "FLOAT",
            "float64": "DOUBLE PRECISION",
            "date": "DATE",
            "time": "TIME",
            "datetime": "TIMESTAMP",
            "datetime64": "DATE",
            "timedelta": "INTERVAL",
            "timedelta64": "INTERVAL",
            "bytes": "BYTEA",
            "void": "BYTEA",
            "bool": "BOOLEAN",
            "list": "ARRAY",
            "dict": "JSON",
        }

    @classmethod
    def from_new_connection(
        cls, host: str, user: str, password: str, port: str, database: Optional[str]
    ):
        conn = ps.connect(
            host=host,
            database=database if database else user,
            user=user,
            password=password,
            port=port,
        )
        return cls(conn)

    def create_table(
        self,
        filepath: str,
        table: str,
        index_col: Optional[str] = None,
        datetime_cols: Optional[List[str]] = None,
    ) -> None:
        cur = self.conn.cursor()
        columns = self.create_table_schema(filepath, index_col, datetime_cols)
        sanitized_cols = [
            sql.Identifier(c[0]).as_string(cur) + f" {c[1]}" for c in columns
        ]
        sanitized_cols[0] += " PRIMARY KEY"
        delete_query = sql.SQL("DROP TABLE IF EXISTS {0};").format(
            sql.Identifier(table)
        )
        query = sql.SQL("CREATE TABLE {0} ({1});").format(
            sql.Identifier(table), sql.SQL(",".join(sanitized_cols))
        )
        cur.execute(delete_query)
        cur.execute(query)
        return cur.query.decode("utf-8")

    def upload(
        self,
        filepath: str,
        table: str,
        index_col: Optional[str] = None,
        datetime_cols: Optional[List[str]] = None,
    ):
        """Uploads a CSV file as its own table in a Postgres DB

        Args:
            filepath (str): Path to CSV file
            table (str): Table name
            index_col (Optional[str], optional): Name of index column. Defaults to
            None, in which case a numerical index is used.
            datetime_cols (Optional[List[str]], optional): List of column names for
            "datetime" columns. Defaults to None.
        """
        cur = self.conn.cursor()
        self.create_table(filepath, table, index_col, datetime_cols)
        self.buffer.seek(0)
        # `copy_expert` lets us specify CSV formatting, which is important when columns
        # contain commas, or for keeping null values.
        cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT CSV)", self.buffer)
        self.conn.commit()

    def create_table_schema(
        self,
        filepath: str,
        index_col: Optional[str] = None,
        datetime_cols: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        """Generates a Postgres schema based on the Pandas dtypes of an input CSV file

        Args:
            filepath (str): Path to CSV file
            index_col (Optional[str], optional): Name of index column. Defaults to
            None, in which case a numerical index is used.
            datetime_cols (Optional[List[str]], optional): List of column names for
            "datetime" columns. Defaults to None.

        Returns:
            Dict[str, str]: Mapping from column name to postgres type name
        """
        df = pd.read_csv(filepath, parse_dates=datetime_cols)
        if "Unnamed: 0" in df.columns:
            df.drop("Unnamed: 0", inplace=True, axis=1)
        if not index_col:
            df = df.reset_index()
            index_col = df.columns[0]
        else:
            df.insert(0, index_col, df.pop(index_col), allow_duplicates=True)

        df = df.infer_objects()
        df.to_csv(self.buffer, header=False, index=False)
        cols_map = self.map_sql_dtypes(df)
        return cols_map

    def map_sql_dtypes(self, df: pd.DataFrame) -> List[Tuple[str, str]]:
        """Given a dataframe, map each of its columns to the appropriate Postgres type

        Args:
            df (pd.DataFrame): Input dataframe

        Returns:
            Dict[str, str]: Mapping from column name to postgres type name
        """
        col_to_pgtype = []
        for col in df.columns:
            dtype = df[col].dtype.name

            if "time" in dtype and re.compile(r"\[.*\]$").search(dtype):
                dtype = re.sub(r"\[.*\]$", "", dtype)
            col_to_pgtype.append((col, self.pg_2_sql_map[dtype]))

        return col_to_pgtype
