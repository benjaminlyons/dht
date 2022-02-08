import ClusterClient
import sys
import time

NAME = sys.argv[1]
N = int(sys.argv[2])
K = int(sys.argv[3])

client = ClusterClient.ClusterClient(N, K)
client.connect(NAME)

# insert objects
fastest_insert = 10000000000
fastest_remove = 10000000000
slowest_insert = 0
slowest_remove = 0
for i in range(500):
    start = time.time()
    client.insert("key" + str(i), "value" + str(i))
    stop = time.time()
    elapsed = stop - start
    if elapsed < fastest_insert:
        fastest_insert = elapsed
    if elapsed > slowest_insert:
        slowest_insert = elapsed

    start = time.time()
    client.remove("key" + str(i))
    stop = time.time()
    elapsed = stop - start
    if elapsed < fastest_remove:
        fastest_remove = elapsed
    if elapsed > slowest_remove:
        slowest_remove = elapsed

print("slowest insert:", slowest_insert)
print("fastest insert:", fastest_insert)

print("slowest remove:", slowest_remove)
print("fastest remove:", fastest_remove)
