## CQSH NOTES ##

# Get keyspaces
SELECT * FROM system_schema.keyspaces;

# Get tables
SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'ks1';

# Get table info
SELECT * FROM system_schema.columns WHERE keyspace_name = 'ks1' AND table_name = 'song_playlist_session';



DESCRIBE TABLE song_plays;

INSERT INTO song_plays(artist) VALUES('HELLO');