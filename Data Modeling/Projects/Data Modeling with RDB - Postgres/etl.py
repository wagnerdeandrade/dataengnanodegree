import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def __insert_song_data(cur, df):
    """Insert song data into songs table.

    Arguments:
    cur -- cursor to an open connection the database where the data will be inserted into.
    df -- pandas DataFrame containing the data that will be inserted.
    """
    song_data = (
        df.song_id.values[0],
        df.title.values[0],
        df.artist_id.values[0],
        (df.year.values[0]).item(),
        (df.duration.values[0]).item()
    )
    cur.execute(song_table_insert, song_data)
    
def __insert_artist_data(cur, df):
    """Insert artist data into artists table.

    Arguments:
    cur -- cursor to an open connection the database where the data will be inserted into.
    df -- pandas DataFrame containing the data that will be inserted.
    """
    artist_data = (
        df.artist_id.values[0],
        df.artist_name.values[0],
        df.artist_location.values[0],
        (df.artist_latitude.values[0]).item(),
        (df.artist_longitude.values[0]).item()
    )
    cur.execute(artist_table_insert, artist_data)
    
def process_song_file(cur, filepath):
    """Reads a JSON song description file containing song data like artist, song duration,
    song name and more. Use this data to populate Sparkify songs and artists database tables.

    Arguments:
    cur -- cursor to an open connection the database where the data will be inserted into.
    filepath -- path to the file containing song information.
    """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    __insert_song_data(cur, df)
    
    # insert artist record
    __insert_artist_data(cur, df)
    
    
def __insert_time_data(cur, df):
    """Insert date information into time table.

    Arguments:
    cur -- cursor to an open connection the database where the data will be inserted into.
    df -- pandas DataFrame containing the data that will be inserted.
    """
    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # create a dataframe with corresponding database values
    time_data = (
        t,
        t.dt.hour.values,
        t.dt.day.values,
        t.dt.weekofyear.values,
        t.dt.month.values,
        t.dt.year.values,
        t.dt.weekday.values
    )
    column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame.from_dict(
        dict(
            zip(column_labels, time_data)
        )
    )

    # insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    
def __insert_user_data(cur, df):
    """Insert user information into users table.

    Arguments:
    cur -- cursor to an open connection the database where the data will be inserted into.
    df -- pandas DataFrame containing the data that will be inserted.
    """
    # load user dataframe
    user_df = pd.concat([df.userId, df.firstName, df.lastName, df.gender, df.level], axis=1)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
    # free dataframe
    user_df = user_df.iloc[0:0]
    
def __insert_songplay_data(cur, df):
    """ Given a table of songplay events, for each songlay event, search the
    songid and artistid on songs and artists database tables, populating the songplays table
    with the songplay data, the song and the artist IDs of the music being played.

    Arguments:
    cur -- cursor to an open connection the database where the data will be inserted into.
    df -- pandas DataFrame containing the data that will be inserted.
    """
    # for each songplay event, described by a row in the dataframe
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
    
def process_log_file(cur, filepath):
    """Reads a log JSON file containing events information, extracts NextSong (music listening)
    events and populate Sparkify database time, user and songplay tables based on those events.

    Arguments:
    cur -- cursor to an open connection the database where the data will be inserted into.
    filepath -- path to the file containing song information.
    """
  
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action - i.e. get only listening music events from the logs
    df = df[(df.page == "NextSong")]

    # insert time records
    __insert_time_data(cur, df)
    
    # insert user records
    __insert_user_data(cur, df)
    
    # insert songplay records
    __insert_songplay_data(cur, df)
    
    # erase dataframe
    df = df.iloc[0:0]

def process_data(cur, conn, filepath, func):
    """ Navigate through the filepath directory structure executing func for each 
    found file.

    Arguments:
    cur -- cursor to an open connection the database where the data will be inserted into.
    conn -- open connection to the database.
    filepath -- directory path to read the files.
    func -- function to be executed for each found file.
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

    # connect to Sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")

    # get a cursor to operate on Sparkify open database
    cur = conn.cursor()
    
    # process all song files
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)

    # process all log files
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    # close database connection
    conn.close()


if __name__ == "__main__":
    main()