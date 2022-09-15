# pgsql-csv-uploader

A simple excursion into writing, packaging, and uploading a python library to PyPI.
A friend of mine was trying to find a fast way to upload a CSV to a Postgres database, and ran into some issues.
Firstly, Postgres requires the table schema to be determined in advance, and doesn't have a way to natively infer column data types.
Libraries like SQLAlchemy provide systems to do this -- however, the SQLAlchemy ORM is substantially slower than interacting with the database
driver directly (which is done by libraries like psycopg2). As a result, there was a (very small, very specific) need to create a smoother interface
for uploading CSVs to Postgres quickly and with some kind of type inference.

Note that this is a *very* limited library. It requires being able to load the dataset into memory, and currently doesn't do any kind of partitioning
(uses a Pandas DataFrame as an intermediary to perform the type inference) or support parallelism. It also requires superadmin privileges within the DB.
However, it does work decently well on small datasets, and is more of an exercise in the whole package-creation process anyway :) 
