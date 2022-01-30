#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# #### Import Python packages 

# In[1]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


# checking your current working directory
print("\n----------------------------------------------------------------------")
print(f"Gathering data from {os.getcwd()}/event_data")

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
print("Creating a smaller event data csv file called event_datafile_full.csv that will be used to insert data into the Apache Cassandra tables")


# In[4]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(f"Created event_datafile_full.csv with {sum(1 for line in f)} rows")


# # Part 2. Building the tables for each of the queries
# 
# - Give me the artist, song title and song's length in the music app history that was heard during `sessionId = 338` and `itemInSession  = 4`
# - Give me only the following: name of artist, song (sorted by `itemInSession`) and `user` (first and last name) for `userid = 10` and `sessionid = 182`
# - Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

# #### Now we are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# #### Creating a Cluster

# In[5]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
try:
    cluster = Cluster()
    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
except Exception as e:
    print(e)


# #### Create Keyspace

# In[6]:


# Create Keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sparkify_cassandra 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace

# In[7]:


# Set keyspace
try:
    session.set_keyspace('sparkify_cassandra')
    print("Connected to Cassandra and keyspace created")
except Exception as e:
    print(e)


# In[8]:


# Instantiate a list of tables
tables=[]


# #### Query 1: Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

# In[9]:


# Query 1:  
# Give me the artist, song title and song's length in the music app history that 
# was heard during sessionId = 338, and itemInSession = 4

tables.append('session_details')
print("\n----------------------------------------------------------------------")
print(f"Creating a table {tables[0]} to hold the session details with partition key=(sessionId, itemInSession)")
# Build a table to hold the data with partition key=(sessionId, itemInSession)   
# Primary key will allow quick seach on those two fields
query=f"CREATE TABLE IF NOT EXISTS {tables[0]} "
query = query + "(sessionId int, itemInSession int, artist text, song text, length float, PRIMARY KEY ((sessionId, itemInSession)))"
try:
    session.execute(query)
except Exception as e:
    print(e)                    


# In[11]:


# Import the appropriate data into the table.
# This is really slow.  Using CQL COPY FROM would be faster, but I have a bike to fix so not implementing it here 

print(f"Inserting data into {tables[0]}")

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        
        sessionId=line[8]
        itemInSession=line[3]
        # replace ' with '' in text strings in Cassandra
        artist=line[0].replace("'","''")
        song=line[9].replace("'","''")
        length=line[5]
        
        query = f"INSERT INTO {tables[0]} (sessionId, itemInSession, artist, song, length)"
        query = query + f"VALUES ({sessionId}, {itemInSession}, '{artist}', '{song}', {length})"

        session.execute(query)


# #### Do a SELECT to verify that the data have been inserted into each table

# In[13]:


# Check that we can run the query we want against the table
print(f"Checking that the query runs against {tables[0]}")
query=f"SELECT sessionId, itemInSession, artist, song, length from {tables[0]} WHERE sessionID = 338 AND itemInSession = 4"

print(query+"\n")
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print (row.sessionid, row.iteminsession, row.artist, row.song, row.length)


# ### Query 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

# In[14]:


## Query 2. Give me only the following: name of artist, song (sorted by itemInSession) 
# and user (first and last name) for userid = 10, sessionid = 182

tables.append('user_sessions')
print("\n----------------------------------------------------------------------")
print(f"Creating a table {tables[0]} to hold the user sessions with primary key=((sessionId, userId), itemInSession)")

# Build a table to hold the data with partition key=((sessionId, userId), itemInSession)
# Primary key consists of unique partition key plus the clustering column for ordering
query=f"CREATE TABLE IF NOT EXISTS {tables[1]} "
query = query + "(sessionId int, userId int, itemInSession int, artist text, song text, user text, PRIMARY KEY ((sessionId, userId), itemInSession))"

try:
    session.execute(query)
except Exception as e:
    print(e)                    


# In[15]:


# Import the data itno the table.
# This is really slow.  Using CQL COPY FROM would be faster, but I have a bike to fix so not implementing it here

print(f"Inserting data into {tables[1]}")

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        
        sessionId=line[8]
        itemInSession=line[3]
        userId=line[10]
        artist=line[0].replace("'","''")
        song=line[9].replace("'","''")
        firstName=line[1].replace("'","''")
        lastName=line[4].replace("'","''")
        
        query = f"INSERT INTO {tables[1]} (sessionId, userId, itemInSession, artist, song, user)"
        query = query + f"VALUES ({sessionId}, {userId}, {itemInSession}, '{artist}', '{song}', '{firstName} {lastName}')"

        session.execute(query)


# #### Do a SELECT to verify that the data have been inserted into each table

# In[16]:


# Check the query runs against the table
print(f"Checking that the query runs against {tables[1]}")
query=f"SELECT sessionId, userId, itemInSession, artist, song, user FROM {tables[1]} WHERE sessionID = 182 AND userId = 10"
print(query+"\n")

try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print (row.artist, row.song, row.user)


# #### Query 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

# In[17]:


## Query 3. Give me every user name (first and last) in my music app history 
# who listened to the song 'All Hands Against His Own'

print("\n----------------------------------------------------------------------")
tables.append('songs_users')
print(f"Creating a table {tables[2]} to hold the song and user data with primary key= (song, userId)")

# Build a table to hold the data with partition key=(song, userId)
# Primary key consists of unique partition key - we just want one row per song/user combination 
# userId used instead of lastname, fist name as I susepct searching integers is quicker than searching strings
# though as it is hashed that may not be the case.
query=f"CREATE TABLE IF NOT EXISTS {tables[2]} "
query = query + "(song text, userId int, lastName text, firstName text, PRIMARY KEY (song, userId))"

try:
    session.execute(query)
except Exception as e:
    print(e)                    


# In[19]:


# Import the data itno the table.
# This is really slow.  Using CQL COPY FROM would be faster, but I have a bike to fix so not implementing it here
print(f"Inserting data into {tables[2]}")

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:

        userId=line[10]
        song=line[9].replace("'","''")
        firstName=line[1].replace("'","''")
        lastName=line[4].replace("'","''")
        
        query = f"INSERT INTO {tables[2]} (song, userId, lastName, firstName)"
        query = query + f"VALUES ('{song}', {userId}, '{lastName}', '{firstName}')"
        
        session.execute(query)


# #### Do a SELECT to verify that the data have been inserted into each table

# In[20]:


# Check the query runs against the table
print(f"Checking that the query runs against {tables[2]}")
query=f"SELECT song, userId, lastName, firstName FROM {tables[2]} WHERE song = 'All Hands Against His Own'"
print(query+"\n")

# Different approach taken here to allow the data to be sorted by last name (without using lastname as a clustering column for reason given above)

# from https://stackoverflow.com/questions/41247345/python-read-cassandra-data-into-pandas
# NOTE flaw in this method recorded by JosiahJohnston (last post)
# it does not impact us here but worth noting

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

session.row_factory = pandas_factory
session.default_fetch_size = None

rows = session.execute(query, timeout=None)
df_songs_users = rows._current_rows

# wrangle the dataframe to output the song, then the users ordered by surname
df_songs_users['user']=df_songs_users.firstname + " " + df_songs_users.lastname
df_songs_users=df_songs_users.sort_values(by='lastname')

print(df_songs_users[['song', 'user']].to_string(index=False))


# ### Drop the tables before closing out the sessions

# In[21]:


# Drop the tables if they exist

print("\n----------------------------------------------------------------------")
for table in tables:
    query=f"DROP TABLE IF EXISTS {table}"
    try:
        print(f"Dropping {table}")
        session.execute(query)
    except Exception as e:
        print(e) 


# ### Close the session and cluster connection¶

# In[22]:


# clean up
print("\n----------------------------------------------------------------------")
print("Clean up")
session.shutdown()
cluster.shutdown()
os.remove("event_datafile_new.csv")   
