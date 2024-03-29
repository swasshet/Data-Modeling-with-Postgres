import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """    Processes the song file from song_data directory to insert data into two tables: song_data and artist_data
    Args:
    - cur: Allows to executes queries in  Postgres database
    - filepath: File to be Loaded into  Postgres tables
    
    Returns:
    None
    """
     # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[[ 'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """    Processes the log file from log_data directory to insert data into two table: time and user 
    Args:
    - cur: Allows to executes queries in  Postgres database
    - filepath: File to be Loaded into  Postgres tables
    
    Returns:
    None
    """
    # open log file
    df =  pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour , t.dt.day , t.dt.dayofweek , t.dt.month , t.dt.year , t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    time_dict = {"start_time":t , 
             "hour":t.dt.hour,
             "day":t.dt.day,
             "week":t.dt.dayofweek,
             "month":t.dt.month,
             "year":t.dt.year,
             "weekday":t.dt.weekday
            }
        
    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName','lastName','gender','level']]

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
        songplay_data = (index, pd.to_datetime(row.ts,unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
      """
    Process the data in all the files in data directory and creates Postgres tables
    
    Args:
        - cur: Allows to execute Postgres command
        - conn: Establishes connection to Postgres database
        - filepath: Directory containing all the files processed into Postgres tables
        - func: Specifies which function should be executed, can take two values based on the file we are processing.
            + process_song_data: when filepath is song_data, or
            + process_log_data: when filepath is log_data
    
    Returns:
    None
    """
    "get all files matching extension from directory"
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()