# Postgres CSV Uploader

A friend of mine was trying to find a fast way to upload a CSV to a Postgres database, and ran into some issues.
Firstly, Postgres requires the table schema to be determined in advance, and doesn't have a way to natively infer column data types.

Libraries like SQLAlchemy provide systems to do this -- however, the SQLAlchemy ORM is substantially slower than interacting with the database driver directly (which is done by libraries like psycopg2). As a result, there was a (very small, very specific) need to create a smoother interface for uploading CSVs to Postgres quickly and with some kind of type inference.

Note that this is a *very* limited library, which I wrote in about a total of 2 hours. It requires being able to load the dataset into memory, and currently doesn't do any kind of partitioning (uses a Pandas DataFrame as an intermediary to perform the type inference) or support parallelism. It also requires superadmin privileges within the DB. However, it does work decently well on small datasets.

## Example Usage:
    from postgres_csv_uploader.uploader import PostgresCSVUploader
    import psycopg2 as ps

    conn = ps.connect(
        host="hostname",
        user="username",
        password="password",
        port=5432
    )
    uploader = PostgresCSVUploader(conn)
    uploader.upload(
        "my_file.csv",
        "my_table_name",
        index_col="uid",
        datetime_cols=["first_date", "second_date"]
    )
