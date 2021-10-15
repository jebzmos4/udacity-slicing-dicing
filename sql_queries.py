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
    CREATE TABLE staging_events
(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
);
""")

time_table_create = ("""
CREATE TABLE time 
(
  t_start_time  TIMESTAMP PRIMARY KEY,
  t_hour   INTEGER NOT NULL,
  t_day      INTEGER NOT NULL,
  t_week        INTEGER NOT NULL,
  t_month      INTEGER NOT NULL,
  t_year    INTEGER NOT NULL,
  t_weekday   INTEGER NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE users 
(
  u_user_id    INTEGER PRIMARY KEY,
  u_first_name VARCHAR(22) NOT NULL,
  u_last_name  VARCHAR(22) NOT NULL,
  u_gender     VARCHAR(7) NOT NULL,
  u_level      VARCHAR(7) NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE artists 
(
  a_artist_id  INTEGER PRIMARY KEY,
  a_name   VARCHAR(22) NOT NULL,
  a_location      VARCHAR(7) NOT NULL,
  a_lattitude     FLOAT NOT NULL,
  a_longitude     FLOAT NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE songs 
(
  s_song_id    INTEGER PRIMARY KEY,
  s_title      VARCHAR(22) NOT NULL,
  s_artist_id  INTEGER NOT NULL,
  s_year       INTEGER NOT NULL,
  s_duration   FLOAT NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE songplays 
(
  p_songplay_id  INTEGER IDENTITY(0,1) PRIMARY KEY,
  p_start_time   TIMESTAMP,
  p_user_id      INTEGER NOT NULL,
  p_level        VARCHAR(7) NOT NULL,
  p_song_id      INTEGER NOT NULL,
  p_artist_id    INTEGER NOT NULL,
  p_session_id   INTEGER NOT NULL,
  p_location     VARCHAR(25) NOT NULL,
  p_user_agent   VARCHAR(25) NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {} 
    credentials 'aws_iam_role={}'
    json {}
    compupdate off
    region 'us-west-2';
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    copy staging_songs 
    from {} 
    credentials 'aws_iam_role={}' 
    JSON 'auto' truncatecolumns 
    compupdate off
    region 'us-west-2';
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM
    staging_events
WHERE
    user_id IS NOT NULL AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM
    staging_songs
WHERE
    song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    name,
    location,
    latitude,
    longitude
FROM
    staging_songs
WHERE
    artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    start_time,
    EXTRACT(HOUR from start_time) AS hour,
    EXTRACT(DAY from start_time) AS day,
    EXTRACT(WEEK from start_time) AS week,
    EXTRACT(MONTH from start_time) AS month,
    EXTRACT(YEAR from start_time) AS year,
    EXTRACT(DAYOFWEEK from start_time) AS weekday
FROM
    songplays;
""")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
    se.userId AS user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId AS session_id,
    se.location,
    se.userAgent AS user_agent
FROM staging_events se
JOIN staging_songs ss
ON se.artist = ss.artist_name
AND se.song = ss.title
WHERE se.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, artist_table_create, user_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
