# 
# Udacity Data Engineering Nanodegree 
# Tom Baird
# Jan 2022
#
# Code to read data and import it into postgres tables
#
# Version History
# 0.1    2022-01-27    TB    Initial version
# 0.2    2022-01-29    TB    Update to rename the song_select query to song_detail_sql and clarify the purpose of the query 

# Fact Table
# songplays - records in log data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
#
# Dimension Tables
# users - users in the app
# user_id, first_name, last_name, gender, level
# songs - songs in music database
# song_id, title, artist_id, year, duration
# artists - artists in music database
# artist_id, name, location, latitude, longitude
# time - timestamps of records in songplays broken down into specific units
# start_time, hour, day, week, month, year, weekday


# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS songs;"
artist_table_drop = "DROP table IF EXISTS artists;"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE table IF NOT EXISTS songplays (
songplay_id SERIAL PRIMARY KEY
, start_time timestamp REFERENCES time
, user_id int REFERENCES users
, level varchar
, song_id varchar 
, artist_id varchar REFERENCES artists
, session_id int
, location varchar
, user_agent varchar
, constraint no_dups unique (start_time
, user_id
, level
, song_id
, artist_id
, session_id
, location
, user_agent)
, FOREIGN KEY(song_id, artist_id) REFERENCES songs(song_id, artist_id)
, FOREIGN KEY(start_time) REFERENCES time(start_time));
""")

user_table_create = ("""
CREATE table IF NOT EXISTS users (
user_id int PRIMARY KEY
, firstname varchar
, lastname varchar
, gender char(1)
, level varchar);   
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar NOT NULL
, title varchar
, artist_id varchar NOT NULL REFERENCES artists
, year int
, duration decimal(9,5)
, PRIMARY KEY(song_id, artist_id));
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY
, name varchar
, location varchar
, latitude decimal(8,5) CHECK (latitude BETWEEN -90 and 90) 
, longitude decimal(9,5)  CHECK (latitude BETWEEN -179.99999 and 180));
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY
, hour int
, day int
, week int
, month int
, year int
, weekday int);
""")

# INSERT RECORDS

songplay_table_insert = ("""
COPY songplays (
start_time
, user_id
, level
, song_id
, artist_id
, session_id
, location
, user_agent) FROM '/home/workspace/tmp/tmp_data.csv'  
(DELIMITER(','), FORMAT csv);
""")

user_table_insert = ("""
    COPY users FROM '/home/workspace/tmp/tmp_data.csv'  
    (DELIMITER(','), FORMAT csv);
""")

song_table_insert = ("""
COPY songs FROM '/home/workspace/tmp/tmp_data.csv'  
    (DELIMITER(','), FORMAT csv);
""")

artist_table_insert = ("""
COPY artists FROM '/home/workspace/tmp/tmp_data.csv'  
    (DELIMITER(','), FORMAT csv);
""")


time_table_insert = ("""
COPY time FROM '/home/workspace/tmp/tmp_data.csv'  
    (DELIMITER(','), FORMAT csv);
""")

# FIND SONGS
# for each song get song_id, artist_id, title, artist name, and duration
# (this will allow a join with the log data on title, artist name, and duration)
song_detail_sql = ("""
select song_id, songs.artist_id, artists.name, songs.title, songs.duration
FROM songs 
INNER JOIN artists 
ON songs.artist_id = artists.artist_id
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# COLUMN LISTS
artist_columns=['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
song_columns=['song_id', 'title', 'artist_id', 'year','duration']
time_columns=['ts','hour','day','week_of_year','month','year','weekday']
user_columns=['userId', 'firstName', 'lastName', 'gender', 'level','ts']