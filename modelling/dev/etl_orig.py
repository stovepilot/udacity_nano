import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def get_files(filepath):
    """
    This is the docstring placeholder
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def insert_artist_data(cur, file_path):

    song_files = get_files(file_path)
    
    song_files
    quit()
    
#     artist_columns=['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
#     artist_data = pd.DataFrame()
#     for f in song_files:
#         tmp_df=pd.read_json(f, typ='series')[artist_columns]
#         artist_data = artist_data.append(tmp_df, ignore_index=True)
    
#     artist_data=artist_data[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].drop_duplicates()
    
#     artist_data.to_csv('artist_data.csv', index=False, header = False)

#     sql="""COPY artists FROM '/home/workspace/artist_data.csv'  
#     (DELIMITER(','), FORMAT csv)"""
#     cur.execute(sql)
#     conn.commit() 

    
def insert_song_data(cur, song_files):

    song_columns=['song_id', 'title', 'artist_id', 'year','duration']
    song_data = pd.DataFrame()
    for f in song_files:
        tmp_df=pd.read_json(f, typ='series')[song_columns]
        song_data = song_data.append(tmp_df, ignore_index=True)
    song_data.head(1)
    
    song_data['year']=song_data['year'].astype(int).astype(str)
    song_data=song_data[['song_id', 'title', 'artist_id', 'year', 'duration']].drop_duplicates()
    
    song_data.to_csv('song_data.csv', index=False, header = False)
    sql="""COPY songs FROM '/home/workspace/song_data.csv'  
    (DELIMITER(','), FORMAT csv)"""
    print(sql)
    cur.execute(sql)
    conn.commit() 


def process_log_file(cur, filepath):
    """
    This is the docstring placeholder
    """
    # open log file
    df = 

    # filter by NextSong action
    df = 

    # convert timestamp column to datetime
    t = 
    
    # insert time data records
    time_data = 
    column_labels = 
    time_df = 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This is the docstring placeholder
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    This is the docstring placeholder
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()