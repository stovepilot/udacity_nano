import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession SMALLINT,
        lastName VARCHAR,
        length FLOAT,
        level CHAR(4),
        location VARCHAR,
        method CHAR(3),
        page VARCHAR,
        registration FLOAT,
        sessionId SMALLINT,
        song VARCHAR,
        status SMALLINT,
        ts  BIGINT,
        userAgent VARCHAR,
        userId SMALLINT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs SMALLINT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year SMALLINT
    );
""")

songplay_table_create = ("""
    CREATE table IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY
        , start_time timestamp REFERENCES time
        , user_id int REFERENCES users 
        , level varchar
        , song_id varchar 
        , artist_id varchar REFERENCES artists
        , session_id int
        , location varchar
        , user_agent varchar
        , constraint no_dups unique (
            start_time
            , user_id
            , level
            , song_id
            , artist_id
            , session_id
            , location
            , user_agent
        )
        , FOREIGN KEY(song_id, artist_id) REFERENCES songs(song_id, artist_id)
        , FOREIGN KEY(start_time) REFERENCES time(start_time)
    ) DISTKEY(user_id);
""")

user_table_create = ("""
    CREATE table IF NOT EXISTS users (
        user_id int PRIMARY KEY
        , firstname varchar
        , lastname varchar
        , gender char(1)
        , level varchar
    ) DISTKEY(user_id);   
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar NOT NULL 
        , title varchar
        , artist_id varchar NOT NULL REFERENCES artists
        , year int
        , duration decimal(9,5)
        , PRIMARY KEY(song_id, artist_id)
    )DISTKEY(song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY
        , name varchar
        , location varchar
        , latitude decimal(8,5)
        , longitude decimal(9,5)
    )DISTKEY(artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY
        , hour int
        , day int
        , week int
        , month int
        , year int
        , weekday int
    ) DISTSTYLE AUTO;
""")

# STAGING TABLES
# Cribbed from here https://knowledge.udacity.com/questions/628520
staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        format json as 's3://udacity-dend/log_json_path.json';
""")

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data'
        credentials 'aws_iam_role={}'
        format as json 'auto' compupdate off region 'us-west-2';
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id,
        location,
        user_agent
    )
    SELECT

        timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' AS start_time,
        userId as user_id,
        level,
        song_id,
        artist_id,
        sessionid,
        location,
        userAgent
    FROM staging_events se
    INNER JOIN (    
        SELECT song_id, songs.artist_id, artists.name, songs.title, songs.duration
        FROM songs 
        INNER JOIN artists 
        ON songs.artist_id = artists.artist_id    
    ) as sd
        ON se.length = sd.duration
        AND UPPER(se.artist) = UPPER(sd.name)
        AND UPPER(se.song) = UPPER(sd.title);
       
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT 
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM staging_events
    WHERE page='NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration 
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT 
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude 
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT 
        timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' AS timestamp, 
        DATE_PART (hour, timestamp) as hour, 
        DATE_PART (day, timestamp) as day, 
        DATE_PART(week, timestamp) as week_of_year,
        DATE_PART (month, timestamp) as month, 
        DATE_PART(year, timestamp) as year, 
        DATE_PART(dayofweek, timestamp) as weekday 
    FROM staging_events
    WHERE page='NextSong';
""")    

songplay_table_counts = ("""
    select
        count(*) as rows,
        count(songplay_id) as songplays,
        count(distinct sp.artist_id) as artists,
        count(distinct sp.song_id) as songs,
        count(distinct sp.user_id) as users
    from songplays sp
    INNER JOIN time
        on sp.start_time=time.start_time
    INNER JOIN users
        on sp.user_id = users.user_id
    INNER JOIN artists
        on artists.artist_id = sp.artist_id
    INNER JOIN songs
        on songs.song_id = sp.song_id;
""")

# Unused in final code but useful for sense checks during dev
songplay_table_select = ("""
    select
        songplay_id,
        time.year,
        month,
        day,
        hour,
        firstname + ' ' + lastname as user,
        title,
        name
    from songplays sp
    INNER JOIN time
        on sp.start_time=time.start_time
    INNER JOIN users
        on sp.user_id = users.user_id
    INNER JOIN artists
        on artists.artist_id = sp.artist_id
    INNER JOIN songs
        on songs.song_id = sp.song_id
    order by 2,3,4,5 asc
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [artist_table_insert, song_table_insert, user_table_insert, time_table_insert, songplay_table_insert]
check_table_queries = [songplay_table_counts]
