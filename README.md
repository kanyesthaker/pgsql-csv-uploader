# Postgres CSV Uploader

This package provides a simple utility to upload CSV files to a Postgres DB without explicitly specifying the table schema in advance by inferring the column types from the corresponding Pandas dtypes.

Libraries such as SQLAlchemy provide systems to do this -- however, the SQLAlchemy ORM is substantially slower than interacting with the database driver directly (which is done by libraries like psycopg2). As a result, there was a (very small, very specific) need to create a smoother interface for uploading CSVs to Postgres quickly and with some kind of type inference.

Note that this is a *very* limited library. It requires being able to load the dataset into memory, and currently doesn't do any kind of partitioning or support parallelism. It also requires superadmin privileges within the DB. However, it does work decently well on small datasets.

As a word of caution, uploading is a *destructive* operation, meaning that it will *overwrite* the the existing table specified by the `table_name` parameter if it exists.

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