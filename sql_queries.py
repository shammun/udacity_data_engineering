import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
DWH_IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')

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
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts VARCHAR,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs 
    (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration DECIMAL,
        year INTEGER
    );
    """)

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY,
        user_id INTEGER NOT NULL REFERENCES users(user_id),
        level VARCHAR,
        song_id VARCHAR NOT NULL REFERENCES songs(song_id),
        artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
        session_id INTEGER NOT NULL,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id INTEGER SORTKEY PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender CHAR(1),
        level VARCHAR NOT NULL
    ) distyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id VARCHAR SORTKEY PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL DISTKEY REFERENCES artists(artist_id),
        year INTEGER NOT NULL,
        duration DECIMAL NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists 
    (
        artist_id VARCHAR(60) SORTKEY PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        location VARCHAR(300),
        latitude DECIMAL,
        longitude DECIMAL
    ) distyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
        hour NUMERIC NOT NULL,
        day NUMERIC NOT NULL,
        week NUMERIC NOT NULL,
        month NUMERIC NOT NULL,
        year NUMERIC NOT NULL,
        weekday NUMERIC NOT NULL
    ) distyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {} region 'us-west-2' iam_role '{}' compupdate off statupdate off FORMAT AS json {}
""").format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {} region 'us-west-2' iam_role '{}' compupdate off statupdate off FORMAT AS json 'auto'
""").format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, user_agent, location)
        SELECT DISTINCT(timestamp 'epoch' + se.ts/1000 * interval '1 second') AS start_time,
            ss.userId AS user_id,
            ss.level AS level,
            ss.song_id AS song_id,
            ss.artist_id AS artist_id,
            ss.sessionId AS session_id,
            ss.userAgent AS user_agent,
            ss.location AS location
        FROM staging_songs ss
        JOIN staging_events se ON (ss.artist_name = se.atist_name AND ss.title = se.song)
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
        SELECT DISTINCT(se.usedrId) AS user_id,
            se.firstName AS first_name,
            se.lastName AS last_name,
            se.gender AS gender,
            se.level AS level
        FROM staging_event se
        WHERE se.userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
        SELECT DISTINCT(ss.song_id) AS song_id,
            ss.title AS title,
            ss.artist_id AS artist_id,
            ss.year AS year,
            ss.duration AS duration
    FROM staging_songs ss
    
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
        SELECT DISTINCT(ss.artist_id) AS artist_id,
            ss.artist_name AS name,
            ss.artist_loation AS location,
            ss.artist_latitude AS latitude,
            ss.artist_longitude AS longitude
        FROM staging_songs AS ss
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT(s.start_time) AS start_time,
            EXTRACT(hour FROM s.start_time) AS hour,
            EXTRACT(day FROM s.start_time) AS day,
            EXTRACT(week FROM s.start_time) AS week,
            EXTRACT(month FROM s.start_time) AS month,
            EXTRACT(year FROM s.start_time) AS year,
            EXTRACT(dow FROM s.start_time) AS weekday
        FROM songplays s
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
