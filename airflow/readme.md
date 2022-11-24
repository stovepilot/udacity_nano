# Project: Data pipelies using Airflow

## Rationale

The point of the pipeline is to move data from S3 into a set of fact and dimension tables - performing DQ  as part of the process and 

## Design

The pipeline is designed to take data from S3 and ingest it into a pair of staging tables in a Redshift database.  The data is then transformed into a set of Fact nad Dimension tables suitable for analytical queries.  

### Database

The database  design and schema is pre-configured as part of the course.  The tables are created at runtime

### DAGS

#### Defaults

The default values are set as follows (as per the project instructions)
```
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),    
}
```
The DAG is set to run each hour with only one run active at any one time

#### Tasks

- Dummy begin and end tasks are set as marker points
- A pair of staging tasks are set up.  They use the stagin operator (see below).
- Five load tasks are set up for the fact table and the four dimension tables.  They use the load operators (see below).
  - songplays (with the append parameter set to True so new data is appended each hour).
  - songs
  - artists
  - users
  - time
- A pair of DQ tasks are set up - one to check the staging tables and one to check the fact and dimension tables

#### Task Dependencies

- The tables are created first up
- The data is then ingested into the staging tables (these two tasks run in parallel).
- DQ checks are performed
- The data is loaded into the fact and deimension tables (in parallel)
- DQ checks are performed

### Operators

#### stage_redshift

Takes parameters for the s3 bucket, the expected file format,  the region and other parameters, and the credentials

#### load_fact

Takes a table name, a SQL string contianing the SELECT query that defines what will be inserted, and the connection details.  There is also a flag controlling whether data is inserted or appended.

#### load_dimension

Takes a table name, a SQL string contianing the SELECT query that defines what will be inserted, and the connection details.  There is also a flag controlling whether data is inserted or appended.

#### data_quality

Runs a simple check that verifes that the number of rows is above what is expected.  The expectations can be set as part of the task (though they are currently set to 1).

### Helpers

The helper scripts give the transformation SQL.  They are provided as parrt of hte set up and are unchanged.







