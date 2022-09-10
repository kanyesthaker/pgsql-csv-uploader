from src.dbwriter import DBWriter
import psycopg2 as ps
import pandas as pd
from datetime import datetime
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
        "index": ["id1", "id2", "id3"],
        "text": ["hello", "world", "goodbye"],
        "int": [1, 2, 3],
        "float": [1.5, 2.5, 3.5],
        "datetime": [datetime.now(), datetime.today(), datetime.utcnow()]   
    })
    fp = tmp_path_factory.getbasetemp() / 'test_data_small.csv'
    df.to_csv(fp)
    return fp


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
        ('index', 'VARCHAR'),
        ('text', 'VARCHAR'),
        ('int', 'INTEGER'),
        ('float', 'FLOAT'),
        ('datetime', 'DATE')
    ]

def test_typemap_large(conn, mock_dataset_large):
    dbwriter = DBWriter(conn)
    df = pd.read_csv(mock_dataset_large, parse_dates=['sale_date'])
    typemap = dbwriter.map_sql_dtypes(df)
    print(typemap)
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

def test_get_pgsql_schema(conn, mock_dataset_small):
    dbwriter = DBWriter(conn)
    no_index_schema = dbwriter.get_pgsql_schema(mock_dataset_small, datetime_cols=['datetime'])
    assert no_index_schema == [
        ("Unnamed: 0","INTEGER"),
        ("index", "VARCHAR"),
        ("text", "VARCHAR"),
        ("int", "INTEGER"),
        ("float", "FLOAT"),
        ("datetime", "DATE"),
    ]
    index_schema = dbwriter.get_pgsql_schema(mock_dataset_small, index_col = 'index', datetime_cols=['datetime'])
    assert index_schema == [
        ("index", "VARCHAR"),
        ("text", "VARCHAR"),
        ("int", "INTEGER"),
        ("float", "FLOAT"),
        ("datetime", "DATE"),
    ]

def test_create_table(conn, mock_dataset_small):
    dbwriter = DBWriter(conn)
    fp = mock_dataset_small
    table = 'mock_table'
    target_query = (
        'CREATE TABLE "mock_table" ('
            '"Unnamed: 0" INTEGER PRIMARY KEY,'
            '"index" VARCHAR,'
            '"text" VARCHAR,'
            '"int" INTEGER,'
            '"float" FLOAT,'
            '"datetime" DATE'
        ');'
    )
    assert dbwriter.create_table(
        fp,
        table,
        datetime_cols=['datetime']
    ) == target_query
