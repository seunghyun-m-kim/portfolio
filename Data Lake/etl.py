import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    song_df.createOrReplaceTempView("songdata")
    songs_table = spark.sql("""
    SELECT song_id,
           title,
           artist_id,
           year,
           duration
    FROM
           (SELECT song_id,
                   title,
                   artist_id,
                   year,
                   duration,
                   max(counter)
            FROM
                   (SELECT song_id,
                           title,
                           artist_id,
                           year,
                           duration,
                           ROW_NUMBER() OVER(ORDER BY song_id) AS counter
                    FROM songdata)
            GROUP BY song_id,
                     title,
                     artist_id,
                     year,
                     duration)
    """)

    # write songs table to parquet files partitioned by year and artist
    songs_table.coalesce(1).write.csv(path=output_data, header=True, mode='append')

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT artist_id,
           artist_name AS name,
           artist_location AS location,
           artist_latitude AS latitude,
           artist_longitude AS longitude
    FROM
           (SELECT artist_id,
                   artist_name,
                   artist_location,
                   artist_latitude,
                   artist_longitude,
                   max(counter)
            FROM
                   (SELECT artist_id,
                           artist_name,
                           artist_location,
                           artist_latitude,
                           artist_longitude,
                           ROW_NUMBER() OVER(ORDER BY artist_id) AS counter
                    FROM songdata)
            GROUP BY artist_id,
                     artist_name,
                     artist_location,
                     artist_latitude,
                     artist_longitude)
    """)

    # write artists table to parquet files
    artists_table.coalesce(1).write.csv(path=output_data, header=True, mode='append')



def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    log_df = spark.read.json(log_data)

    # extract columns for users table
    log_df.createOrReplaceTempView("logdata")
    users_table = spark.sql("""
    SELECT userId AS user_id,
           firstName AS first_name,
           lastName AS last_name,
           gender,
           level
    FROM   (SELECT userId,
                   firstName,
                   lastName,
                   gender,
                   level,
                   MAX(ts)
            FROM logdata
            GROUP BY userId,
                     firstName,
                     lastName,
                     gender,
                     level)
    """)

    # write users table to parquet files
    users_table.coalesce(1).write.csv(path=output_data, header=True, mode='append')


    # extract columns to create time table
    time_table = spark.sql("""
    SELECT DISTINCT ts AS start_time,
                    HOUR(CAST(from_unixtime(ts/1000) AS timestamp)) AS hour,
                    DAYOFMONTH(CAST(from_unixtime(ts/1000) AS timestamp)) AS day,
                    WEEKOFYEAR(CAST(from_unixtime(ts/1000) AS timestamp)) AS week,
                    MONTH(CAST(from_unixtime(ts/1000) AS timestamp)) AS month,
                    YEAR(CAST(from_unixtime(ts/1000) AS timestamp)) AS year,
                    DAYOFWEEK (CAST(from_unixtime(ts/1000) AS timestamp)) AS weekday
    FROM logdata
    """)

    # write time table to parquet files partitioned by year and month
    time_table.coalesce(1).write.csv(path=output_data, header=True, mode='append')


    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
    SELECT ld.ts,
           ld.userid,
           ld.level,
           sd.song_id,
           sd.artist_id,
           ld.sessionid,
           ld.location,
           ld.useragent
    FROM logdata ld
    JOIN songdata sd
    ON ld.song = sd.title AND
       ld.artist = sd.artist_name AND
       ld.length = sd.duration
    WHERE ld.page = 'NextSong'
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.coalesce(1).write.csv(path=output_data, header=True, mode='append')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    spark.stop()

if __name__ == "__main__":
    main()
