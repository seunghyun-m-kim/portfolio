import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_BUCKET=config.get('S3','LOG_DATA')
SONG_BUCKET=config.get('S3','SONG_DATA')
JSON=config.get('S3','LOG_JSONPATH')
ARN=config.get('IAM_ROLE','ARN')


# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplay"
user_table_drop = "DROP table IF EXISTS user_table"
song_table_drop = "DROP table IF EXISTS song"
artist_table_drop = "DROP table IF EXISTS artist"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS
staging_events (artist varchar, auth varchar, firstname varchar, gender varchar, iteminsession varchar, lastname varchar, length numeric, level varchar, location varchar, method varchar, page varchar, registration numeric, sessionid numeric, song varchar, status numeric, ts numeric, useragent varchar, userid numeric)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS
staging_songs (ss_id bigint identity(0,1), song_id varchar, num_songs numeric, title varchar, artist_name varchar, artist_latitude numeric, year numeric, duration numeric, artist_id varchar, artist_longitude numeric, artist_location varchar)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS
songplays (songplay_id bigint identity(0,1), start_time numeric, user_id varchar, level varchar, song_id varchar, artist_id varchar, session_id numeric, location varchar, user_agent varchar, primary key (songplay_id));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS
users (user_id varchar, first_name varchar, last_name varchar, gender varchar, level varchar, primary key(user_id));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS
songs (song_id varchar, title varchar, artist_id varchar, year numeric, duration numeric, primary key (song_id));
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS
artists (artist_id varchar, name varchar, location varchar, latitude numeric, longitude numeric, primary key (artist_id));
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS
time (start_time numeric, hour numeric, day numeric, week numeric, month numeric, year numeric, weekday numeric, primary key (start_time))
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
JSON {};
""".format(LOG_BUCKET, ARN, JSON))

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
JSON 'auto';
""".format(SONG_BUCKET, ARN))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT se.ts,
       se.userid,
       se.level,
       ss.song_id,
       ss.artist_id,
       se.sessionid,
       se.location,
       se.useragent
FROM staging_events se
JOIN staging_songs ss
ON se.song = ss.title AND
   se.artist = ss.artist_name AND
   se.length = ss.duration
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT userid,
       firstname,
       lastname,
       gender,
       level
FROM
       (SELECT userid,
               firstname,
               lastname,
               gender,
               level,
               max(ts)
         FROM staging_events
         GROUP BY userid,
                  firstname,
                  lastname,
                  gender,
                  level)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
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
               max(ss_id)
        FROM staging_songs
        GROUP BY song_id,
                 title,
                 artist_id,
                 year,
                 duration)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
FROM
       (SELECT artist_id,
               artist_name,
               artist_location,
               artist_latitude,
               artist_longitude,
               max(ss_id)
        FROM staging_songs
        GROUP BY artist_id,
                 artist_name,
                 artist_location,
                 artist_latitude,
                 artist_longitude)
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
                EXTRACT(hour from (TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second')),
                EXTRACT(day from (TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second')),
                EXTRACT(week from (TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second')),
                EXTRACT(month from (TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second')),
                EXTRACT(year from (TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second')),
                EXTRACT(weekday from (TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second'))
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
