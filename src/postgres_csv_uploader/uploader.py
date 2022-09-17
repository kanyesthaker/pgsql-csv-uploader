from inspect import signature
from typing import Dict, List, Optional, Tuple
from io import StringIO
import numpy as np
import psycopg2 as ps
from psycopg2.extensions import connection
import psycopg2.sql as sql
import pandas as pd
from sqlalchemy.dialects import postgresql
from collections import defaultdict
import warnings
import re

class PostgresCSVUploader:
    
    def __init__(self, conn: connection):
        self.conn = conn
        self.data = None
        self.buffer = StringIO()
    
    @classmethod
    def from_new_connection(cls,
        host: str,
        user: str,
        password: str,
        port: str,
        database: Optional[str]
    ):
        conn = ps.connect(
            host=host,
            database=database if database else user,
            user=user,
            password=password,
            port=port
        )
        return cls(conn)
    
    def create_table(
        self, 
        filepath: str, 
        table: str,
        index_col: Optional[str] = None,
        datetime_cols: Optional[List[str]] = None
    ) -> None:
        cur = self.conn.cursor()
        columns = self.create_table_schema(filepath, index_col, datetime_cols)
        sanitized_cols = [sql.Identifier(c[0]).as_string(cur) + f" {c[1]}" for c in columns]
        sanitized_cols[0] += " PRIMARY KEY"
        delete_query = sql.SQL('DROP TABLE IF EXISTS {0};').format(sql.Identifier(table))
        query = sql.SQL(
            "CREATE TABLE {0} ({1});"
            ).format(
                sql.Identifier(table),
                sql.SQL(",".join(sanitized_cols))
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
            index_col (Optional[str], optional): Name of index column. Defaults to None, in which case a numerical index is used.
            datetime_cols (Optional[List[str]], optional): List of column names for "datetime" columns. Defaults to None.
        """
        cur = self.conn.cursor()
        self.create_table(filepath, table, index_col, datetime_cols)
        self.buffer.seek(0)
        cur.copy_from(self.buffer, table, sep=',')
        self.conn.commit()

    def create_table_schema(
        self,
        filepath: str,
        index_col: Optional[str] = None,
        datetime_cols: Optional[List[str]] = None
    ) -> Dict[str, str]:
        """Generates a Postgres schema based on the Pandas dtypes of an input CSV file

        Args:
            filepath (str): Path to CSV file
            index_col (Optional[str], optional): Name of index column. Defaults to None, in which case a numerical index is used.
            datetime_cols (Optional[List[str]], optional): List of column names for "datetime" columns. Defaults to None.

        Returns:
            Dict[str, str]: Mapping from column name to postgres type name
        """
        df = pd.read_csv(filepath, parse_dates=datetime_cols)
        if "Unnamed: 0" in df.columns: df.drop("Unnamed: 0", inplace=True, axis=1)
        if not index_col:
            df = df.reset_index()
            index_col = df.columns[0]
        else:
            df.insert(0, index_col, df.pop(index_col), allow_duplicates=True)
        
        df = df.infer_objects()
        df.to_csv(self.buffer, header=False, index=False)
        cols_map = self.map_sql_dtypes(df)
        return cols_map

    def _get_pysql_typemap(self) -> Dict[str, List[str]]:
        """Maps basic python types to SQLAlchemy Postgres types by finding base Python type

        Returns:
            Dict[Type, List]: A map from Python type to an equivalent List of Postgres types
        """
        d = defaultdict(list)
        for key in postgresql.__dict__['__all__']:
            sqltype = getattr(postgresql, key)
            if 'python_type' in dir(sqltype) and not sqltype.__name__.startswith('Type'):
                paramlist = signature(sqltype.__init__).parameters
                base_type = sqltype() if 'item_type' not in paramlist else sqltype(None) 
                try:
                    d[base_type.python_type.__name__].append(key)
                except NotImplementedError: pass

        return dict(d)

    def _get_nppy_typemap(self) -> Dict[str, str]:
        """Maps numpy types to python types

        Returns:
            Dict[str, str]: A map from each numpy type to the associated Python type
        """
        mapping = defaultdict()
        for name in dir(np):
            try:
                obj = getattr(np, name)
                if hasattr(obj, 'dtype'):
                    # Catch numpy deprecation warnings since we're not actually using those types
                    # We just want their names
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        if 'time' in name:
                            mapping[np.dtype(name).name] = type(obj(0, 'D').item()).__name__
                        else:
                            mapping[np.dtype(name).name] = type(obj(0).item()).__name__
            except: continue
        return mapping

    def map_sql_dtypes(self, df: pd.DataFrame) -> List[Tuple[str, str]]:
        """Given a dataframe, map each of its columns to the appropriate Postgres type 

        Args:
            df (pd.DataFrame): Input dataframe

        Returns:
            Dict[str, str]: Mapping from column name to postgres type name
        """
        col_to_pgtype = []
        nppy_typemap = self._get_nppy_typemap()
        pysql_typemap = self._get_pysql_typemap()
        for col in df.columns:
            if df[col].dtype.name == 'object':
                dtype = 'str'
            else:
                dtype = df[col].dtype.name

            if 'time' in dtype and re.compile(r'\[.*\]$').search(dtype):
                dtype = re.sub(r'\[.*\]$', '', dtype)
            pytype = nppy_typemap[dtype]
            sql_type = pysql_typemap[pytype]
            col_to_pgtype.append((col, sql_type[0]))
        return col_to_pgtype
