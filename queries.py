# Set KEYSPACE to the keyspace specified above
from data_utils.cassandraConnection import createCassandraConnection
from data_utils.cassandra_utils import createKeySpace
from data_utils.query_utils import execute_query, query_to_df, insert_statement
import csv
import pandas as pd
dbsession = createCassandraConnection()

createKeySpace("ks1",dbsession)
try:
    dbsession.set_keyspace('ks1')
except Exception as e:
    print(e)

# query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
# essionId = 338, and itemInSession = 4
query1 = """CREATE TABLE IF NOT EXISTS song_playlist_session (
            sessionId int,
            itemInSession int,
            artist text, 
            song text, 
            length float, 
            PRIMARY KEY (sessionId, itemInSession))"""

# execute the CREATE query
execute_query(query1, dbsession)

file = 'event_datafile_new.csv'
# INSERT query
query2 = """INSERT INTO song_playlist_session (
            sessionId, itemInSession, artist, song, length)
            VALUES (%s, %s, %s, %s, %s) """
# dict, key: colIndex, value: data type to be casted
dict1 = {8: int, 3: int, 0: str, 9: str, 5: float}

# call insert_statement function to insert certain values from file
insert_statement(file, query2, dict1, dbsession)