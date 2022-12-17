# Capstone Project

## TL/DR - just get me started

----
## Rationale

Ingest of I94 immigration data to a set of tables in a Redshift database for analytical purposes.  The (imaginary) use case requires the user to be able to analyse immigration data on an adhoc basis - with a focus on particular ports of entry.  It is envisaged that the users will interact withte data using the 

## Approach

A combination of Spark and SQL was use for this project; the data is staged on S3 and stored in a Reshift database.

## Data Exploration
See detail in the ata Cleaning section below

## Design

### Cleaning
The data is clean using pySpark a set of pySaprk modules whch read the source data into dataframes and wrangle it.  These are run locally - this decisiosn was taken as it was quicker and easier to implement and doe not incur compute costs.

The cleaned data is written back to the local filesystem as either parquet files (immigration data) or csv files (dimention data).

### S3 buckets

The S3 bucket is created via boto3.

### Data upload

The data is uploaded from the local filesystem to the S3 bucket to using Spark.

### Database design

A star schema is used for the database with the immigration data held in the fact table and various dimension tables  joined to it via foreign keys.

## Data Wrangling
### Immigration Data
#### Null values
There are multiple NULL values throughout the dataset but these have been left unchagned as they are categorical data fields - interpolating data to fill them would not be a valid approach

#### Arrival and Departure dates
Arrival and departure dates are held in SAS format - these are converted to datetime fields

#### Additional columns in the Jun datafile
Four additional features are present in the June data.  These were all NULL so the features have been dropped.

### Airport data
The most obvious source of the airport data is the airport_codes_cvs.csv file.  However, though the data is of good quality investigations showed that it is incomplete.  On that basis the SAS label file was used.


## Code

#### aws.cfg
Contains all cofiguration settings apart from the aws credentials, which are held separately

### Wrangling
#### clean_data.py
Contains functions to clean the data and write it back to a location set in the config file (currently the local filesystem).

### Data Upload
#### upload_data.py
A set of functions to create the s3 bucket (only ever run once) and upload the data from the local filesystem, to s3.  This latter code would be run for each monthly/daily dataset for the job were to be scheduled.

### Redshift tools
#### redshift.py
A set of functions which allow the redshift cluster to be spun up, paused, restarted or deleted entirely whcih configuration based on the config file.

### ETL 
#### etl.py
A set of functions used to create and populate the database on the redshift cluster.  The SQL called by the functions is held separately

#### sql_queries.py
The sql called by the fucntions on etl.py

### User control code
The followling code is designed to be called by the user to perform the various tasks (and could indeed be called from a tool like airflow).

#### main.py

#### start_cluster.py

#### pause_cluster.py

#### delete_cluster.py


## test.py
### unit tests

## sample_queries

#### sample_queries.sql

