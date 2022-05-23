import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

exec(open("create_tables.py").read())

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def process_song_file(cur, filepath):
    # open song file
    song_files = get_files("data/song_data")
    for i in range(len(song_files)):
        filepath = song_files[i]
        df = pd.read_json(filepath, lines=True)

    # insert song record
    song_values = df.values

    artist_id = song_values[0][0]
    duration = song_values[0][5]
    song_id = song_values[0][7]
    title = song_values[0][8]
    year = song_values[0][9]

    song_data = [song_id, title, artist_id, year, duration] 
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_values = df.values

    artist_id = artist_values[0][0]
    name = artist_values[0][4]
    location = artist_values[0][2]
    latitude = artist_values[0][1]
    longitude = artist_values[0][3]

    artist_data = [artist_id, name, location, latitude, longitude]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    log_files = get_files("data/log_data")
    for i in range(len(log_files)):
        df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = df['ts']
    loop = t.head()
    # print(loop)
    time_data = []
    for milli in t:
        seconds=int(milli/1000)
        minutes=int(seconds/60)
        hours=int(minutes/60)
        days=int(hours/24)
        months=int(days/30)
        years=int(days/365)
        seconds = seconds%60
        minutes = minutes%60
        hours = hours%24
        temp_time = str(hours) + ":" + str(minutes) + ":" + str(seconds)
        days=days%30
        months=months%12
        temp_date = str(years) + "-" + str(months) + "-" + str(days)
        temp_both = temp_date + " " + temp_time
        time_data.append(temp_both)

    # print(time_data)
    time_data = pd.to_datetime(time_data)
    # print(time_data)
    
    # insert time data records
    time_data = (df['ts'], time_data.hour, time_data.day, time_data.weekofyear, time_data.month, time_data.year, time_data.weekday)

    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')

    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_info = (df['userId'], df['firstName'], df['lastName'], df['gender'], df['level'])
    column_labels=('user_id', 'first_name', 'last_name', 'gender', 'level')

    user_df = pd.DataFrame(dict(zip(column_labels, user_info)))

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
        songplay_values = df.values
        songplay_id = index
        timestamp = cur.execute("SELECT start_time FROM time")
        user_id = songplay_values[0][17]
        level = songplay_values[0][7]
        song_id = cur.execute ("SELECT song_id FROM songs")
        artist_id = cur.execute("SELECT artist_id FROM artists")
        session_id = songplay_values[0][12]
        location = songplay_values[0][8]
        user_agent = songplay_values[0][16]

        songplay_data = [songplay_id, timestamp, user_id, level, song_id, artist_id, session_id, location, user_agent]

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()