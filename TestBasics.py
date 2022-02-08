import ClusterClient
import json
import sys
import re

NAME = sys.argv[1]
N = int(sys.argv[2])
K = int(sys.argv[3])

# create client
client = ClusterClient.ClusterClient(N, K)

# connect to server
client.connect(NAME)
print("Successfully connected to server.")
print()

# insert sample objects
client.insert("apple", {"test": "apple pie"})
client.insert("banana", {"test": "banana pudding"})
client.insert('cranberry','arbitrary string')
client.insert('date', 5)
client.insert('elderberry', [1, 2, 3, 4, 5])
print("Inserted objects with keys apple, banana, cranberry, date, and elderberry")

try:
    client.insert(None, {"test": "cranberry sauce"})
except ValueError:
    print("Insert object with None type for key:", sys.exc_info()[0], sys.exc_info()[1])

try:
    client.insert(5, {"test": "cranberry sauce"})
except ValueError:
    print("Insert object with non-string key:", sys.exc_info()[0], sys.exc_info()[1])
print()

# test lookup
print("lookup('apple'):", client.lookup("apple"))
print("lookup('banana'):", client.lookup("banana"))
print("lookup('cranberry'):", client.lookup("cranberry"))
print("lookup('date'):", client.lookup("date"))
print("lookup('elderberry'):",client.lookup("elderberry"))
try:
    client.lookup("app")
except KeyError:
    print('client.lookup("app"):', sys.exc_info()[0], sys.exc_info()[1])

try:
    client.lookup(None)
except ValueError:
    print('client.lookup(None):', sys.exc_info()[0], sys.exc_info()[1])

print()

# test scan
print("client.scan('apple'):",client.scan("apple"))
print("client.scan('a'):", client.scan("a"))
print("client.scan('not found pattern'):", client.scan('not found pattern'))
try:
    print(client.scan(None))
except ValueError:
    print("client.scan(None):", sys.exc_info()[0], sys.exc_info()[1])

try:
    print(client.scan("lkm(lmk}"))
except re.error:
    print("Invalid regex client.scan('lkm(lmk}'):", sys.exc_info()[0], sys.exc_info()[1])

print()

# test remove
print("remove('apple'):", client.remove("apple"))
try:
    client.lookup("apple")
except KeyError:
    print('client.lookup("apple"):', sys.exc_info()[0], sys.exc_info()[1])
try:
    client.remove("apple")
except KeyError:
    print('client.remove("apple"):', sys.exc_info()[0], sys.exc_info()[1])
try:
    client.remove(None)
except ValueError:
    print('client.remove(None):', sys.exc_info()[0], sys.exc_info()[1])
print()

# client.close()
