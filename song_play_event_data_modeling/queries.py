# Set KEYSPACE to the keyspace specified above
from data_utils.cassandra_utils import createKeySpace, createCassandraConnection
from data_utils.query_utils import query_to_df, insert_statement, execute_query
import csv
import pandas as pd

dbsession = createCassandraConnection()

createKeySpace("ks1", dbsession)
try:
    dbsession.set_keyspace('ks1')
except Exception as e:
    print(e)


def insert_statement(file, query, insert_dict, session):
    """
    This function reads csv file, extracts certain values from
    file by executing query, and casts certain data type of each
    column of data in line

    Parameters:
        file: file name ended with .csv  E.g. filename = "data123.csv"
        query: CQL query with INSERT statement
        insert_dict: Python dictionary format,
                     Key: column index in csv file
                     Value: casted type
                     E.g. dict = {2: int} which refers to the third column of data
                                 to be casted by Integer
    """
    with open(file, encoding='utf8') as f:
        csvreader = csv.reader(f)  # create reader object
        next(csvreader)  # skip header
        for line in csvreader:
            session.execute(query, [y(line[x]) for x, y in insert_dict.items()])


# query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
# essionId = 338, and itemInSession = 4
query1 = """CREATE TABLE IF NOT EXISTS song_playlist_session (
            sessionId int,
            itemInSession int,
            artist text, 
            song text, 
            length float, 
            PRIMARY KEY (sessionId, itemInSession))"""


create_song_table_query = """CREATE TABLE IF NOT EXISTS song_plays(
                            artist text, 
                            PRIMARY KEY (artist))
                            """
execute_query(create_song_table_query, dbsession)

insert_song_play_event_query="""
INSERT INTO song_plays(arist) VALUES (%s);
"""
file = 'event_datafile_new.csv'
d = {0:str}

with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)  # create reader object
    next(csvreader)  # skip header
    for line in csvreader:
        dbsession.execute(insert_song_play_event_query, [cast_variable_type(line[column]) for column, cast_variable_type in d.items()])


file = 'event_datafile_new.csv'
# INSERT query
query2 = """INSERT INTO song_playlist_session (
            sessionId, itemInSession, artist, song, length)
            VALUES (%s, %s, %s, %s, %s) """
# # dict, key: colIndex, value: data type to be casted
dict1 = {8: int, 3: int, 0: str, 9: str, 5: float}

#dict1 = {'sessionId': }

# call insert_statement function to insert certain values from file
insert_statement(file, query2, dict1, dbsession)
