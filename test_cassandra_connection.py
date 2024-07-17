from cassandra.cluster import Cluster

try:
    cluster = Cluster(['172.19.0.5'])
    session = cluster.connect()
    print('Connected to Cassandra')
except Exception as e:
    print(f'Failed to connect to Cassandra: {e}')