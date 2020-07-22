import configparser

# CONFIGURATION FILE  
## Contains AWS environment configuration informations. 
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM", "ARN")


# CREATE TABLES QUERIES

## STAGING TABLES 

###  Such tables are important to avoid inefficient reading datasource files line by line,
### they play a central role in ETL, using this approach we can read an entire file using
### a single COPY command, much quicker process to populate the database.
staging_events_table_create= ("""
                                CREATE TABLE IF NOT EXISTS stg_events (
                                    artist        text,
                                    auth          text,
                                    firstName     text,
                                    gender        varchar(1),
                                    itemInSession int,
                                    lastName      text,
                                    length        decimal,
                                    level         text,
                                    location      text,
                                    method        text,
                                    page          text,
                                    registration  bigint,
                                    sessionId     int,
                                    song          text,
                                    status        int,
                                    ts            timestamp,
                                    userAgent     text,
                                    userId        int
                                );
                              """)

staging_songs_table_create = ("""
                                CREATE TABLE IF NOT EXISTS stg_songs (
                                    num_songs        int,
                                    artist_id        text,
                                    artist_latitude  float,
                                    artist_longitude float,
                                    artist_location  text,
                                    artist_name      text,
                                    song_id          text,
                                    title            text,
                                    duration         decimal,
                                    year             int
                                );
                              """)


## STAGING TABLES POPULATE QUERIES
staging_events_copy = ("""
                          COPY stg_events
                          FROM 's3://udacity-dend/log_data' 
                          iam_role {}
                          JSON 's3://udacity-dend/log_json_path.json'
                          timeformat 'epochmillisecs';
                        """).format(ARN)

staging_songs_copy = ("""
                         COPY stg_songs
                         FROM 's3://udacity-dend/song_data' 
                         iam_role {}
                         JSON 'auto';
                      """).format(ARN)



## OLAP TABLES

### Star schema tables where the Analytical data will reside on.
### Distribution style is based on SONGID.
###    From Sparkify problem description we have that the purpose of this database is:
###    "... analytics team ... finding insights in what songs their users are listening to..."
###    Therefore we may think that queries will be heavy on songs and users, as there may
###    be more songs than users in the world, better to align the songs in the same distribution as
###    the fact table to avoid heavy shuffling due to even distributions. 

### Sort keys are also used on relevant elements, not too much to avoid DWH burden but some
### to enjoy the sorting benefits on enhancing DWH response times.

#### FACT TABLE
songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id int       IDENTITY(0,1) PRIMARY KEY NOT NULL,
                                start_time  timestamp NOT NULL      sortkey,
                                user_id     int       NOT NULL,
                                level       text, 
                                song_id     text      NOT NULL      distkey,
                                artist_id   text      NOT NULL,
                                session_id  int       NOT NULL, 
                                location    text, 
                                user_agent  text
                             );   
                         """)

#### DIMENSION TABLES
user_table_create = ("""
                        CREATE TABLE IF NOT EXISTS users (
                            user_id    int PRIMARY KEY NOT NULL,
                            first_name text            NOT NULL,
                            last_name  text,
                            gender     varchar(1),
                            level      text
                         )
                         diststyle all;   
                     """)

song_table_create = ("""
                        CREATE TABLE IF NOT EXISTS songs (
                            song_id   text PRIMARY KEY NOT NULL distkey,
                            title     text             NOT NULL,
                            artist_id text             NOT NULL sortkey,
                            year      int,
                            duration  decimal
                         );   
                     """)

artist_table_create = ("""
                          CREATE TABLE IF NOT EXISTS artists(
                              artist_id text PRIMARY KEY NOT NULL sortkey, 
                              name      text             NOT NULL, 
                              location  text, 
                              latitude  float, 
                              longitude float
                           )
                           diststyle all;   
                       """)

time_table_create = ("""
                        CREATE TABLE IF NOT EXISTS time (
                            start_time timestamp PRIMARY KEY NOT NULL sortkey,
                            hour       int                   NOT NULL,
                            day        int                   NOT NULL,
                            week       int                   NOT NULL,
                            month      int                   NOT NULL,
                            year       int                   NOT NULL,
                            weekday    int                   NOT NULL
                        )
                        diststyle all;
                     """)

## OLAP TABLES POPULATE QUERIES

### PS: intriguing the fact that there are more "NextSong" events than songplay rows 
###  => assuming that this is because the songs dataset is not completlly tied to
###  the events (log_data) dataset and therefore although there are a common song set,
###  not all the songs listened on "NextSong" events are directly mapped into a song
###  of Sparkify song dataset.
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
                                stg_events.ts,
                                stg_events.userId,
                                stg_events.level,
                                stg_songs.song_id,
                                stg_songs.artist_id,
                                stg_events.sessionId,
                                stg_events.location,
                                stg_events.userAgent 
                            FROM 
                                stg_events
                            JOIN
                                stg_songs 
                            ON 
                                (stg_events.song = stg_songs.title AND stg_events.artist = stg_songs.artist_name)
                            WHERE 
                                (stg_events.userId IS NOT NULL AND stg_events.page = 'NextSong')
                         """)

user_table_insert = ("""
                        INSERT INTO users (
                            user_id, 
                            first_name, 
                            last_name, 
                            gender, 
                            level
                        ) 
                        SELECT 
                            DISTINCT userId, 
                            firstName, 
                            lastName, 
                            gender, 
                            level
                        FROM stg_events
                        WHERE 
                            stg_events.userId IS NOT NULL;
                     """)

song_table_insert = ("""
                        INSERT INTO songs(
                            song_id, 
                            title, 
                            artist_id, 
                            year, 
                            duration
                        ) 
                        SELECT 
                            DISTINCT song_id,
                            title,
                            artist_id,
                            year,
                            duration
                        FROM stg_songs;
                     """)

artist_table_insert = ("""
                          INSERT INTO artists(
                              artist_id, 
                              name, 
                              location, 
                              latitude, 
                              longitude
                          )
                          SELECT
                              DISTINCT artist_id,
                              artist_name,
                              artist_location,
                              artist_latitude,
                              artist_longitude
                          FROM stg_songs;
                       """)

time_table_insert = ("""
                        INSERT INTO time(
                            start_time,
                            hour,
                            day,
                            week,
                            month,
                            year,
                            weekday
                        )
                        SELECT 
                            DISTINCT ts,
                            date_part(h, ts),
                            date_part(d, ts),
                            date_part(w, ts),
                            date_part(mon, ts),
                            date_part(y, ts),
                            date_part(dow, ts)
                        FROM stg_events;
                     """)

# DROP TABLES QUERIES
staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS stg_songs"

songplay_table_drop       = "DROP TABLE IF EXISTS songplays;"
user_table_drop           = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS songs;"
artist_table_drop         = "DROP TABLE IF EXISTS artists;"
time_table_drop           = "DROP TABLE IF EXISTS time;"

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries   = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries   = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]