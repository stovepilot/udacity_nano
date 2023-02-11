

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
import configparser
import pyspark.sql.functions as F



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']



# Create spark session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()
    return spark

def process_data(spark):

    # Import event data
    # Define the schema for the JSON data
    event_json_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", ByteType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),
    ])


    EVENT_DATA = "s3a://udacity-dend/log_data/*/*/*"


    df_events = spark.read.json(EVENT_DATA, schema=event_json_schema)
    df_events = df_events.filter(df_events.page=='NextSong') 

    # Convert ts to timestamp
    # with reference to https://stackoverflow.com/questions/53537226/pyspark-from-unixtime-not-showing-the-correct-datetime

    df_events = df_events.withColumn(
        'timeStamp',
       (F.from_unixtime(df_events.ts / 1000,"yyyy-MM-dd hh:mm:ss.SSS"))
    )

    # Create columns for day, month, year, etc
    df_events = df_events.withColumn('Hour', F.hour(df_events.timeStamp)).withColumn('day', F.dayofmonth(df_events.timeStamp)).withColumn('week', F.weekofyear(df_events.timeStamp)).withColumn('month', F.month(df_events.timeStamp)).withColumn('year', F.year(df_events.timeStamp)).withColumn('weekday', F.dayofweek (df_events.timeStamp))

    # Create a temp table
    df_events.createOrReplaceTempView("staging_events")

    # Create a dataframe for users
    users = spark.sql("""SELECT DISTINCT 
            userId as user_id,
            firstName as firstname,
            lastName as lastname,
            gender as gender,
            level as level
        FROM staging_events""");


    # Create a df for time
    time = spark.sql("""SELECT DISTINCT timeStamp, hour, day,  week, month, year, weekday FROM staging_events""");


    # Import song data

    SONG_DATA = "s3a://udacity-dend/song_data/*/*/*/*.json"

    # Define the schema for the JSON data
    song_data_json_schema = StructType([
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_name", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("num_songs", IntegerType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True),
    ])

    # REad in the song data
    df_song_data = spark.read.json(SONG_DATA, schema=song_data_json_schema)

    # Check the count
    # df_song_data.count()

    # Create a staging table
    df_song_data.createOrReplaceTempView("staging_songs")

    # Create a songs df
    songs = spark.sql("""SELECT DISTINCT 
            song_id, 
            title,
            artist_id,
            year,
            duration 
        FROM staging_songs
        WHERE song_id is not null
        """);    


    # Create an artists df
    artists = spark.sql("""SELECT DISTINCT 
            artist_id, 
            artist_name as name, 
            artist_location as location, 
            artist_latitude as latitude, 
            artist_longitude as longitude 
        FROM staging_songs
        WHERE artist_id is not NULL;
        """);    


    # Create dataframes for songs and artists
    songs.createOrReplaceTempView("songs")
    artists.createOrReplaceTempView("artists")


    # Create songplays data
    songplays=spark.sql("""SELECT
            timestamp,
            userId as user_id,
            level,
            song_id,
            artist_id,
            sessionid,
            location,
            userAgent,
            se.year,
            se.month
    FROM staging_events se
    INNER JOIN (    
        SELECT song_id, songs.artist_id, artists.name, songs.title, songs.duration
        FROM songs 
        INNER JOIN artists 
        ON songs.artist_id = artists.artist_id    
    ) as sd
        ON se.length = sd.duration
        AND UPPER(se.artist) = UPPER(sd.name)
        AND UPPER(se.song) = UPPER(sd.title)
    """);


    # Set the output location
    OUTPUT_DATA = "s3://stovepilot_output/"

    # Write the data out to s3
    users.write.mode('overwrite').parquet(OUTPUT_DATA+'users/')
    time.write.partitionBy("year","month").mode('overwrite').parquet(OUTPUT_DATA+'time/')
    artists.write.mode('overwrite').parquet(OUTPUT_DATA+'artists/')
    songs.write.partitionBy("year","artist_id").mode('overwrite').parquet(OUTPUT_DATA+'songs/')
    songplays.write.partitionBy("year","month").mode('overwrite').parquet(OUTPUT_DATA+'songplays/')

def main():

    spark = create_spark_session()
    process_data(spark)    


    if __name__ == "__main__":
        main()


