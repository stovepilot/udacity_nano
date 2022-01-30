# NoSQL with Cassandra
## Project description 
Learning project for Udacity DE Nanodegree.  
Code created to gather and denormalize a dataset of Sparkify song and user activity

## Database design: 
DB design based on the three queries we have been provided with 
1. Give me the artist, song title and song's length in the music app history that was heard during `sessionId = 338` and `itemInSession  = 4`
2. Give me only the following: name of artist, song (sorted by `itemInSession`) and `user` (first and last name) for `userid = 10` and `sessionid = 182`
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'Dimension table holding data about each artist

Three tables are created and populated, optimiszed for each of these queries

### session_details
A table to hold the data with partition key=(sessionId, itemInSession).  This primary key will allow quick seach on those two fields

### user_sessions
A table to hold the data with partition key=((sessionId, userId), itemInSession).  The primary key consists of unique partition key plus the clustering column for ordering

### songs_users 
A table to hold the data with partition key=(song, userId). The primary key consists of unique partition key - we just want one row per song/user combination 
userId is used instead of lastname, first name as I _susepct_ searching integers is quicker than searching strings though as it is hashed that may not be the case.
I have gone with it that was as it led to an interesting problem with outputting the query results (and I am here to learn).

## ETL Process: 
This code was provided by Udacity and runs though the files in /event_data to produce a denormalised csv file (event_datafile_new.csv) containing song and user activity data

![Example of event_data_file_new.csv](./images/image_event_datafile_new.jpg "Example of event_data_file_new.csv")

## Building tables for each query
The same approach is take for each query - a table suited to the query is built in Cassandra and populated by looping through event_data_file_new.csv, inserting the appropriate columns from each row.
_NB_ A better (quicker) approach might have been to use Cassandra's COPY FROM statement but I did not have time to look at that.

## Queries
A select queery was run against the tables to check they worked.  Query 1 and 2 were straightforward.  Query 3 was trickier as I wanted the results to be sorted by the users' last names but (rightly or wrongly) I did not want to cluster on that column in Cassandra (see above).  I approached this by writing the query results to a dtaframe and then wrangling the df to getthe output I needed

## Project Repository files
- `project_1b.py` - This is the code to run
- `dev/Project_1B.ipynb` - This is the development code and _might_ be what I needed to submit.  It is funcitonally identical to project_1b.py.

## How To Run the Project
Run `python project_1b.py` _or_ open the jupyter-notebook and run that manually.

## Assoc files
- The event_data folder contains the source data files (not uploaded to the repo).
- The dev folder contains the ipynb used during development.
- The image folder contains images used in the readme.

