

def createKeySpace(keyspace_name, session):
    # Create a Keyspace
    try:
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace_name + " WITH REPLICATION =  { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    except Exception as e:
        print(e)

