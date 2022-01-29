# 
# Udacity Data Engineering Nanodegree 
# Tom Baird
# Jan 2022
#
# Code to read data and import it into postgres tables
#
# Version History
# 0.1   2022-01-27  TB  Initial version
# 0.2   2022-01-29  TB  Altered get_song_details by changing vairable refereence from song_plays
#                       to song_detail_sql.  No functional change.
#

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# 
def get_files(filepath):
    """
    get_files(filepath)
    Takes a filepath as a parameter and creates a list of all the json files in that location (and any descendents)
    Returns the list
    """
    all_files = []

    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
            
    return all_files

def copy_to_table(df, cur, sql):
    """
    copy_to_table(df, cur, sql)
    Takes a dataframe, cursor and postgres 'COPY TO' SQL string
    Exports the df to csv, runs SQL to copy into pg, deleted the csv 
    """

     # Read the df to CSV
    try:
        df.to_csv('/home/workspace/tmp/tmp_data.csv', index=False, header = False)
    except:
        print('Error wrting tmp_csv\n')
        
    # Bulk import the csv into the artist table using the SQL string in sql_queries.py
    try:
        cur.execute(sql)
        print('.... {} rows imported\n'.format(df.shape[0]))
    except:
        print('Error importing to postgress\n')
        
    # clean up
    os.remove("/home/workspace/tmp/tmp_data.csv")   
    


def insert_artist_data(cur, file_path, columns):
    """
    insert_artist_data(cur, file_path, columns)
    Takes a cursor object, a file path to a folder containg json files, and a list of columns
    Imports those columns    
    """
    
    # Get a list of json files
    song_files = get_files(file_path)
    
    artist_data = pd.DataFrame()
    # Create a df with one row per json file; The columns imported are based on the input parameter
    for f in song_files:
        tmp_df=pd.read_json(f, typ='series')[columns]
        artist_data = artist_data.append(tmp_df, ignore_index=True)
    
    # Drop duplicate rows
    artist_data=artist_data[columns].drop_duplicates()
    
    print('.... {} distinct artists found in the data.'.format(artist_data.shape[0]))
    
    # move the df to postgress
    copy_to_table(artist_data, cur, artist_table_insert)

    
def insert_song_data(cur, file_path, columns):
    """
    insert_song_data(cur, file_path, columns)
    Takes a cursor object, a file path to a folder containg json files, and a list of columns
    Imports those columns 
    """

    # Get a list of json files   
    song_files = get_files(file_path)
    
    song_data = pd.DataFrame()
    # Create a df with one row per json file; The columns imported are based on the input parameter
    for f in song_files:
        tmp_df=pd.read_json(f, typ='series')[columns]
        song_data = song_data.append(tmp_df, ignore_index=True)
    
    # Convert year integers to strings (otherwise the csv file holds them as floats) 
    song_data['year']=song_data['year'].astype(int).astype(str)
    
    # Drop duplicate rows
    song_data=song_data[columns].drop_duplicates()
    
    print('.... {} distinct songs found in the data.'.format(song_data.shape[0]))
    
    # move the df to postgress
    copy_to_table(song_data, cur, song_table_insert)

    
def insert_time_data(cur, log_df):    
    """
    insert_time_data(cur, log_df)
    Takes a cursor object and a dataframe
    Wrangles the df and imports it into pg 
    """
    # Drop duplicates
    time_data = log_df.ts.to_frame().drop_duplicates()
        
    # Derive time/date features from timestamp and add to df 
    # Cribbed from https://stackoverflow.com/questions/34883101/pandas-converting-row-with-unix-timestamp-in-milliseconds-to-datetime
    time_data['hour']=log_df.ts.dt.hour
    time_data['day']=log_df.ts.dt.day
    time_data['week_of_year']= log_df.ts.dt.weekofyear
    time_data['month']=log_df.ts.dt.month
    time_data['year']=log_df.ts.dt.year
    time_data['weekday']=log_df.ts.dt.dayofweek # Monday=0; Sunday=6   
    
    print('.... {} distinct time values found in the data.'.format(time_data.shape[0]))
    
    # move the df to postgress
    copy_to_table(time_data, cur, time_table_insert)
    

def insert_user_data(cur, log_df):  
    """
    insert_user_data(cur, log_df)
    Takes a cursor object and a dataframe
    Wrangles the df, keeping only the latest level value for each user
    Imports it into pg 
    """    
    
    # Select the correct columns
    user_data = log_df[['userId', 'firstName', 'lastName', 'gender', 'level','ts']]
    
    # Conver userId to Int
    user_data=user_data.astype({'userId': 'int'})

    # Sort the df by timestamp
    user_data=user_data.sort_values(by='ts')
    
    # Keep only the latest recrd for each user
    user_data=user_data.drop_duplicates(subset=['userId'],keep='last')
    
    # Drop the ts column
    user_data=user_data.drop(columns=['ts'])
    
    print('.... {} distinct users found in the data.'.format(user_data.shape[0]))
    
    # move the df to postgress
    copy_to_table(user_data, cur, user_table_insert)


def insert_songplay_data(cur, log_df):
    """
    insert_songplay_data(cur, log_df)
    Takes a cursor object and a dataframe containgin log_data
    Merges the log data with song details pulled from postgress to get songplays
    Imports the songplays into pg 
    """        
    song_details = get_song_details(cur)
    
    song_plays = log_df.merge(song_details, how='inner', on=['artist','song','length'])
    
    song_plays=song_plays[['ts','userId','level','song_id','artist_id','sessionId','location','userAgent']]

    print('.... {} distinct songplays found in the data.'.format(song_plays.shape[0]))
              
    # move the df to postgress
    copy_to_table(song_plays, cur, songplay_table_insert)    
    
    
def get_song_details(cur):
    """
    get_song_details(cur)
    Takes a cursor object
    Queries pg db for song details, populates a df and returns the df
    """   

    cur.execute(song_detail_sql)
    data=cur.fetchall()
    
    # From: https://www.linkedin.com/pulse/how-create-pandas-data-frame-postgresql-psycopg-vitor-spadotto
    cols=[]
    for col in cur.description:
        cols.append(col[0])
    song_details=pd.DataFrame(data=data, columns=cols)
    
    song_details.duration=song_details.duration.astype(float)
    song_details=song_details.rename(columns={'name':'artist','duration':'length','title':'song'}) 
    
    return song_details


def process_log_data(cur, file_path, columns=None):
    """
    process_log_data(cur, file_path, columns=None)
    Imports the log files
    Performs minor wrangling
    Passes results to functions which import the log data into the pg tables
    """
    
    # Get the log files
    log_files = get_files(file_path)

    log_df = pd.DataFrame()
    for f in log_files:
        tmp_df=pd.read_json(f, lines=True)
        log_df = log_df.append(tmp_df, ignore_index=True)    
    
    # Remove anythign that is not a songplay
    log_df = log_df[(log_df.page == 'NextSong')]
    
    # convert the timestamp to a datetime
    log_df['ts'] = pd.to_datetime(log_df['ts'], unit='ms')
    
    # Call the functions to insert the log data into the db
    insert_time_data(cur, log_df)
    insert_user_data(cur, log_df)
    insert_songplay_data(cur, log_df)

    
def process_data(cur, conn, filepath, func, columns=None):
    """
    process_data(cur, conn, filepath, func, columns=None)
    Wrapper function to process each dataset
    """
    # get all files matching extension from directory
    all_files=get_files(filepath)

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
  
    func(cur, filepath, columns)


def main():
    """
    main()
    Connects to postgres
    Calls wrapper function to import data
    Closes connection
    """
    
    try:
        # Create a connection to the database and commit automatically
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        conn.set_session(autocommit=True)
        # Create a cursor
        cur = conn.cursor()
        
    except:
        print('Connection Error')
    
    # Create a tmp folder to hold the csv files
    try:
        # Create a tmp folder, if it does not already exist
        if not os.path.exists('/home/workspace/tmp/'):
            os.makedirs('/home/workspace/tmp/')
 #           print('tmp folder created\n')
    except:
        print('Error creating tmp folder')
        
    # Process the artist data in the song data files
    process_data(cur, conn, filepath='data/song_data', func=insert_artist_data, columns=artist_columns)
    # Process the song data in the song data files    
    process_data(cur, conn, filepath='data/song_data', func=insert_song_data, columns=song_columns) 
    # Process the log files
    process_data(cur, conn, filepath='data/log_data', func=process_log_data)     
    
    # Delete the tmp folder
    try:
        # Create a tmp folder, if it does not already exist
        if os.path.exists('/home/workspace/tmp/'):
            os.rmdir('/home/workspace/tmp/')
#            print('tmp folder removed\n')
    except:
        print('Error deleting tmp folder')    
    
    conn.close()

if __name__ == "__main__":
    main()