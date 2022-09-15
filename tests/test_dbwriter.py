from src.dbwriter import DBWriter
import psycopg2 as ps
import psycopg2.sql as sql
import pandas as pd
from datetime import datetime, date
import pytest

@pytest.fixture
def conn_no_db():
    yield ps.connect(
        host="localhost",
        user="kanyesthaker",
        password="admin",
        port=5432,
    )

@pytest.fixture
def mock_db(conn_no_db):
    conn_no_db.autocommit = True
    cur = conn_no_db.cursor()
    cur.execute("DROP DATABASE IF EXISTS mock_db WITH (FORCE);")
    cur.execute("CREATE DATABASE mock_db;")
    yield "mock_db"

@pytest.fixture
def conn(mock_db):
    yield ps.connect(
        host="localhost",
        user="kanyesthaker",
        password="admin",
        port=5432,
        database=mock_db,
    )

@pytest.fixture(scope='session')
def mock_dataset_large(tmp_path_factory):
    src = "http://web.cs.wpi.edu/~cs1004/a16/Resources/SacramentoRealEstateTransactions.csv"
    df = pd.read_csv(src)
    fp = tmp_path_factory.getbasetemp() / 'test_data_large.csv'
    df.to_csv(fp)
    return fp

@pytest.fixture(scope='session')
def mock_dataset_small(tmp_path_factory):
    df = pd.DataFrame({
        "id": ["id1", "id2", "id3"],
        "text": ["hello", "world", "goodbye"],
        "int": [1, 2, 3],
        "float": [1.5, 2.5, 3.5],
        "bool": [True, False, True],
        "datetime": ["Jan 1 1970", "Jan 24 1984", "Sep 14 2022"]   
    })
    fp = tmp_path_factory.getbasetemp() / 'test_data_small.csv'
    df.to_csv(fp)
    return fp

@pytest.fixture(scope='session')
def mock_dataset_very_large(tmp_path_factory):
    df = pd.DataFrame({
        "id": ["id1", "id2", "id3"],
        "bigtext": ["helloworld" * 1000, "helloworld" * 1000, "helloworld" * 1000]
    })
    fp = tmp_path_factory.getbasetemp() / 'test_data_very_large.csv'
    df.to_csv(fp)
    return fp

table_small = "mock_table_small"
table_large = "mock_table_large"
table_very_large = "mock_table_very_large"

def test_dbwriter_init_conn_no_db(conn_no_db):
    dbwriter = DBWriter(conn_no_db)
    assert dbwriter.conn.info.host == "localhost"
    assert dbwriter.conn.info.user == dbwriter.conn.info.dbname == "kanyesthaker"
    assert dbwriter.conn.info.port == 5432

def test_dbwriter_init_conn(conn, mock_db):
    dbwriter = DBWriter(conn)
    assert dbwriter.conn.info.host == "localhost"
    assert dbwriter.conn.info.user == "kanyesthaker"
    assert dbwriter.conn.info.port == 5432
    assert dbwriter.conn.info.dbname == mock_db

def test_dbwriter_init_new(mock_db):
    dbwriter = DBWriter.from_new_connection(
        host="localhost",
        user="kanyesthaker",
        password="admin",
        port=5432,
        database=mock_db
    )
    assert dbwriter.conn.info.host == "localhost"
    assert dbwriter.conn.info.user == "kanyesthaker"
    assert dbwriter.conn.info.port == 5432
    assert dbwriter.conn.info.dbname == mock_db

def test_typemap_small(conn, mock_dataset_small):
    dbwriter = DBWriter(conn)
    df = pd.read_csv(mock_dataset_small, parse_dates=['datetime'])
    assert dbwriter.map_sql_dtypes(df) == [
        ('Unnamed: 0', 'INTEGER'),
        ('id', 'VARCHAR'),
        ('text', 'VARCHAR'),
        ('int', 'INTEGER'),
        ('float', 'FLOAT'),
        ('bool', 'BOOLEAN'),
        ('datetime', 'DATE')
    ]

def test_typemap_large(conn, mock_dataset_large):
    dbwriter = DBWriter(conn)
    df = pd.read_csv(mock_dataset_large, parse_dates=['sale_date'])
    typemap = dbwriter.map_sql_dtypes(df)
    assert typemap == [
        ('Unnamed: 0', 'INTEGER'),
        ('street', 'VARCHAR'),
        ('city', 'VARCHAR'),
        ('zip', 'INTEGER'),
        ('state', 'VARCHAR'),
        ('beds', 'INTEGER'),
        ('baths', 'INTEGER'),
        ('sq__ft', 'INTEGER'),
        ('type', 'VARCHAR'),
        ('sale_date', 'DATE'),
        ('price', 'INTEGER'),
        ('latitude', 'FLOAT'),
        ('longitude', 'FLOAT')
    ]

def test_create_table_schema_no_index(conn, mock_dataset_small):
    dbwriter = DBWriter(conn)
    no_index_schema = dbwriter.create_table_schema(mock_dataset_small, datetime_cols=['datetime'])
    assert no_index_schema == [
        ("index", "INTEGER"),
        ("id","VARCHAR"),
        ("text", "VARCHAR"),
        ("int", "INTEGER"),
        ("float", "FLOAT"),
        ("bool", "BOOLEAN"),
        ("datetime", "DATE"),
    ]

def test_create_table_schema(conn, mock_dataset_small):
    dbwriter = DBWriter(conn)
    index_schema = dbwriter.create_table_schema(mock_dataset_small, index_col='id', datetime_cols=['datetime'])
    assert index_schema == [
        ("id", "VARCHAR"),
        ("text", "VARCHAR"),
        ("int", "INTEGER"),
        ("float", "FLOAT"),
        ("bool", "BOOLEAN"),
        ("datetime", "DATE"),
    ]

def test_create_table(conn, mock_dataset_small):
    dbwriter = DBWriter(conn)
    fp = mock_dataset_small
    target_query = (
        f'CREATE TABLE "{table_small}" ('
            '"id" VARCHAR PRIMARY KEY,'
            '"text" VARCHAR,'
            '"int" INTEGER,'
            '"float" FLOAT,'
            '"bool" BOOLEAN,'
            '"datetime" DATE'
        ');'
    )
    assert dbwriter.create_table(
        fp,
        table_small,
        index_col='id',
        datetime_cols=['datetime']
    ) == target_query

def test_upload_small(conn, mock_dataset_small):
    dbwriter = DBWriter(conn)
    dbwriter.upload(mock_dataset_small, table_small, index_col="id", datetime_cols=['datetime'])
    cur = conn.cursor()
    cur.execute(sql.SQL("SELECT * FROM {0};").format(sql.Identifier(table_small)))
    data = cur.fetchall()
    assert data == [
        ('id1', 'hello', 1, 1.5, True, date(1970, 1, 1)),
        ('id2', 'world', 2, 2.5, False, date(1984, 1, 24)),
        ('id3', 'goodbye', 3, 3.5, True, date(2022, 9, 14))
    ]

def test_upload_large(conn, mock_dataset_large):
    dbwriter = DBWriter(conn)
    dbwriter.upload(mock_dataset_large, table_large, datetime_cols=["sale_date"])
    cur = conn.cursor()
    cur.execute(sql.SQL("SELECT * FROM {0};").format(sql.Identifier(table_large)))
    data = cur.fetchall()
    df = pd.read_csv(mock_dataset_large)

    assert len(data) == len(df)
    assert data[0] == (0, '3526 HIGH ST', 'SACRAMENTO', 95838, 'CA', 2, 1, 836, 'Residential', date(2008, 5, 21), 59222, 38.631913, -121.434879)

def test_upload_very_large(conn, mock_dataset_very_large):
    # Upload a file where a single row is over 8KB, forcing Postgres to TOAST
    dbwriter = DBWriter(conn)
    dbwriter.upload(mock_dataset_very_large, table_very_large, index_col="id")
    cur = conn.cursor()
    cur.execute(sql.SQL("SELECT * FROM {0};").format(sql.Identifier(table_very_large)))
    data = cur.fetchall()
    assert data[0][1] == "helloworld"*1000