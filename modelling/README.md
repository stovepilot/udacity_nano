# Sparkify ETL

## Code
- `create_tables.py` - Code to drop (if necessary) and build a Postgres db to contain Sparkify song and log data
    - This needs to be run first

- `etl.py` - Code to read Sparkify song data and log data from the data directory and and read it into a a postgres database
    - The code comprises four functions
        - one
        - two
        - three
        - four

- `sql_queries.py` - contains the various SQL strings used by the code

## Assoc files

The data folder contains the log and song json files.

The tmp folder is used to hold the temporary csv files that are copied directly into Postgres

The dev folder contains various files used during development