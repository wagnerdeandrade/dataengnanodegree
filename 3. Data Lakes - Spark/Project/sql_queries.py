# SQL queries that using Spark raw data views are responsible to
# create Sparkify OLAP data lake
# Target: given an unstructured set of information as the input,
# i.e. the Sparkify logs about application usage and songs description in JSON
# Create STG view tables containing those information and using it create
# a star schema OLAP data lake of fact table songplays and dimenstion tables
# users, time, artists and songs around it

# DIMENSION TABLES
# Create song dimension table
songs_table_creation = ("""SELECT
                                song_id,
                                title,
                                artist_id,
                                year,
                                duration
                            FROM songs_stg
                        """)

# Create artists dimension table
artists_table_creation = ("""
                            SELECT
                                artist_id,
                                artist_name as name,
                                artist_location as location,
                                artist_latitude as latitude,
                                artist_longitude as longitude
                            FROM songs_stg
                          """)

# Create users dimension table
users_table_creation = ("""
                            SELECT
                                userId as user_id,
                                firstName as first_name,
                                lastName as last_name,
                                gender,
                                level
                            FROM logs_stg
                        """)

# Create time dimension table
time_table_creation = ("""
                            SELECT
                                ts as start_time
                            FROM
                                logs_stg
                       """)

# FACT TABLE
# Create songplays fact table
songplays_table_creation = ("""
                                SELECT
                                    ts as start_time,
                                    userId as user_id,
                                    level,
                                    song_id,
                                    artist_id,
                                    sessionId as session_id,
                                    location,
                                    userAgent as user_agent
                                FROM
                                    logs_stg
                                INNER JOIN
                                    songs_stg
                                ON
                                    (logs_stg.song = songs_stg.title AND logs_stg.artist = songs_stg.artist_name)
                            """)
