import ClusterClient
import time
import json
import sys

NAME = sys.argv[1]
N = 10000
NUM_SERVERS = int(sys.argv[2])
NUM_REPLICAS = int(sys.argv[3])

# connect to server
client = ClusterClient.ClusterClient(NUM_SERVERS, NUM_REPLICAS)
client.connect(NAME)

# generate data for insertions
values = []
for i in range(0, N):
    v = { "test" + str(i) : "value" + str(i) }
    values.append(v)

# construct output string
output_string = ""
# perform insertions
start = time.time()
for i in range(0, N):
    client.insert("key"+str(i), values[i])
end = time.time()
insertion_time = end - start
output_string += "{:.2f}".format(N/insertion_time) + " | "

# perform lookups
start = time.time()
for i in range(0, N):
    try:
        client.lookup("key"+str(i))
    except KeyError:
        pass
end = time.time()
lookup_time = end - start
output_string += "{:.2f}".format(N/lookup_time) + " | "

# perform scan
start = time.time()
for i in range(0, 10):
   client.scan("key.*" + str(i) + ".*")
end = time.time()
scan_time = end - start
# print("Scan throughput:", 10/scan_time, "ops/sec")
output_string += "{:.2f}".format(10/scan_time) + " | "

# perform removes
start = time.time()
for i in range(0, N):
    try:
        client.remove("key" + str(i))
    except KeyError:
        pass
end = time.time()
remove_time = end - start
# print("Remove throughput:", N/remove_time, "ops/sec")
output_string += "{:.2f}".format(N/remove_time) + " | \n"

f = open("REPORT", "a+")
f.write(output_string)
f.close()
