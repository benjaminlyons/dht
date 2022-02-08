# dht
Simple implementation of a distributed hashtable

Made for educational purposes, not for performance.

## Usage
Start at least one HashTableServer with `python HashTableServer.py projectname-0`.
Then connect to the server with 
```
client = ClusterClient(N,K)
client.connect(projectname)
```
where N is the number of HashTableServers and K is the number of data replicas.
