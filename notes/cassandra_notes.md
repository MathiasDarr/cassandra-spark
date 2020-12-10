CAP theorem

Consistency: most recently written/updated data is available.  All nodes in system will return updated data
Unconsistent data may occur when data takes a while to replicate across nodes.

Available:

Partition tolerance: system will be able to function even when some nodes/partitions are down

brewers theorem states that we can only choose two our of three of these
    - partitions always occur,
    - cassandra chooses partition tolerance & availability
    - mysql & psql choose availability & consistent
    
    
    
Write Path

writes are written to any node in the cluster
writes are written to commit log, then to memtable
every write includes a timestamp
memtable flushed to disk periodically
new memtable is created in memory
deletes are a special write cas