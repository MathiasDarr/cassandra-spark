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

create_song_table_query = """CREATE TABLE IF NOT EXISTS song_plays(
                            artist text, 
                            PRIMARY KEY (artist))
                            """

execute_query(create_song_table_query, dbsession)

insert_song_play_event_query="""
INSERT INTO song_plays(artist) VALUES (%s);
"""
file = 'event_datafile_new.csv'
d = {0:str}

with open(file, encoding='utf8') as f:
    csvreader = csv.reader(f)  # create reader object
    next(csvreader)  # skip header
    for line in csvreader:
        dbsession.execute(insert_song_play_event_query, [cast_variable_type(line[column]) for column, cast_variable_type in d.items()])


