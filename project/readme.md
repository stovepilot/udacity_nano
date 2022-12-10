# Capstone Project

## Rationale

Ingest of I94 immigration data to a set of tables in a Redshift database for analytical purposes.

## Approach

## Design

Ingest of I94 immigration data to a set of tables in a Redshift database for analytical purposes.

### Ingest approach

### Database design

Use will need to be able to 

## Data Exploration

### Immigration Data

#### Null values
#### Arrival and Departure dates

### Airport data

The most obvious source of the airport data is the airport_codes_cvs.csv file.  However, though the data is of good quality investigations showed that it is incomplete.

## Data Wrangling

### Code

#### wrangle.py

##### cleanImmigrationFactData(path_to_files)
Takes a path to a dataset, cleans it and writes it back as partitioned parquet files
##### cleanImmigrationDimData(path_to_file)
Takes a file, extracts and cleans the data nd writes it back as csv files

## Data Upload
#### upload.py
##### uploadImmData()


#### uploadFactData()
Do we need a second function?

## iac.py
#### createS3(bucket_name varchar)

#### createRedshift()

#### pauseRedshift()

#### deleteRedshift()

## etl.sql
#### create_immigration.sql
#### airport codes
#### country codes
#### mode codes
#### travel codes
#### visa codes

## ingest.py
#### loadFact()

#### loadDimension()

## test.py
### unit tests

## sample_queries

#### sample_queries.sql

#### demo.ipynb
