# Project: Data Warehousing on AWS

## Rationale

This cloud based ETL process is an adaptation of the oringal on-prem postgres based process.  In order to support a growing user base the processees have been shifted to an AWS Redshift cluster 

## Design

The DB design is relatively simple.  A pair os stagin tables are created to hold the raw data imported from the json files.  A set of analytic tables are then created to support any analytics.

#### Schema

**TODO: Find a way to generate a schema diagram**

##### Distribution
I have followed the principles [here](https://docs.aws.amazon.com/redshift/latest/dg/t_designating_distribution_styles.html) for the distribution keys.  Note that this ignores the [recommended approach in the Redshift documentation](https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html); I have done this in order to demonstrate my understanding of the concepts.  

I have assumed the dataset will grow, and the number of users will far exceed the number of songs or artists.  I have also assumed that the bulkl of the analytic queries will be user based, with a WHERE clause on the user_id.  On that basis I have set the DISTKEY on songplays to the user_id, and set user_id field to be the DISTKEY on the user table.

I have used KEY based distribution on the song and artist tables (``song_id`` and ``artist_id`` respectively) as these fields will be used for the joins.  I also considered using ``DISTSTYLE ALL`` for these tables on the basis that the number of saons and artists might be ver small compared to the songplays, but opted against.

I was not certain what disribution to use for the time table.  I suspect it is to put a distkey on the start time, but I am not sure so I used DISTSTYLE AUTO to let Redshift work out the optimal solution.

##### Sorting
As no analytic queries are provided so I have not added sorting keys to the tables at this stage following principle outlines in the course and [here](https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html) in the Reshift documentation.  I suspect a stragety of sorting songplays on user_id would likely be the best option.

##### Process flow
The run processs, outlined below, kicks off redshift, creates the schema, runs the etl and then stops redshift. 

## Steps

``python redshift_start.py``
``python create_tables.py``
``python etl.py``
``python redshift_stop.py``

The steps of the process are outlined below:

### Fire up Redshiftds

run ``python redshift_start.py``

This reads the config, creates objects, configures an IAM role and a Redshift DB and opens an incoming  TCP port to access the cluster endpoint

### Create Tables 

run ``python create_tables.py``

This drops any existing table and then recreates the schema

### ETL

run ``python etl.py``

This runs through the following steps
#### Populate Staging Tables

Uses copy commands (in ``sql_queries.py``) to populate the staging tables from the appropriate json files ``s3://udacity-dend/log_data`` and ``s3://udacity-dend/song_data``

#### Populate Analytic Tables

Uses copy commands (in ``sql_queries.py``) to populate the database tables from the staging tables

##### Process Song and Artist data

These tables are populated based on the list of distinct songs and artists in the staging_songs table using commands in ``sql_queries.py``

##### Process Songplay data
Processes the staging_events tables, ingoring any events which are not a songplay using commands in ``sql_queries.py``

###### Populate time_table
Timestamps for songplay events are converted from millisecond epoch time to a timestamp and further features (e.g. hour, week) are created 
###### Populate user_table
This table is populated based on the list of distinct users associates with songplay events

###### Populate songplay_table
The songplay table is populated from the staging events table joined to the song and artist tables on artist name, song and aand song duration.
        
### Checks
A simple check is run to show that there are records in the warehouse and that the fact tables join to the dimension tables

### Shut Down Redshift
The Redshift cluster is stopped

run ``python redshift_stop.py``

## Notes