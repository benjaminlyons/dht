from HashTableClient import HashTableClient
import hashlib
import time
import socket
class ClusterClient():
    def __init__(self, num_servers, num_replicas):
        self.num_servers = num_servers
        self.num_replicas = num_replicas
        self.clients = []

    # returns the location of the key in the list of servers
    def _hash_key(self, key):
        if not isinstance(key, str):
            raise ValueError("Key must be a string")
        return int.from_bytes(hashlib.md5(key.encode()).digest(), 'big') % self.num_servers

    def connect(self, name):
        self.main_name = name
        for i in range(self.num_servers):
            server_name = name + "-" + str(i)
            client = HashTableClient()
            client.connect(server_name)
            self.clients.append(client)

    def _attempt_reconnect(self, client_index):
        try:
            self.clients[client_index].close()
            self.clients[client_index] = HashTableClient()
            client = self.clients[client_index]
            client.connect(self.main_name + "-" + str(client_index))
            return True
        except socket.error:
            return False

    # need to add error checking here
    def insert(self, key, value):
        for i in range(0, self.num_replicas):
            success = False
            client_index = (self._hash_key(key) + i) % self.num_servers
            while not success:
                client = self.clients[client_index]
                try:
                    client.insert(key, value)
                    success = True
                except socket.error:
                    # in the event of failure, wait five seconds then reconnect to the server
                    reconnected = False
                    while not reconnected:
                        time.sleep(5)
                        reconnected = self._attempt_reconnect(client_index)
    
    def lookup(self, key):
        need_reconnect = []
        for i in range(0, self.num_replicas):
            need_reconnect.append(False)
        while True:
            for i in range(0, self.num_replicas):
                client_index = (self._hash_key(key) + i) % self.num_servers
                try:
                    if need_reconnect[i]:
                        # print("need reconnect")
                        self._attempt_reconnect(client_index)
                        need_reconnect[i] = False
                    client = self.clients[client_index]
                    # print("lookup")
                    return client.lookup(key)
                except socket.error:
                    # print("set reconnect to true")
                    need_reconnect[i] = True
            time.sleep(5)

    def remove(self, key):
        for i in range(0, self.num_replicas):
            success = False
            client_index = (self._hash_key(key) + i) % self.num_servers
            while not success:
                client = self.clients[client_index]
                try:
                    client.remove(key)
                    success = True
                except socket.error:
                    # in the event of failure, wait five seconds then reconnect to the server
                    reconnected = False
                    while not reconnected:
                        time.sleep(5)
                        reconnected = self._attempt_reconnect(client_index)
                except KeyError:
                    success = True
        return key
    
    def scan(self, regex):
        results = {}
        for i in range(0, self.num_servers):
            success = False
            while not success:
                try:
                    client = self.clients[i]
                    results.update(client.scan(regex))
                    success = True
                except socket.error:
                    reconnected = False
                    while not reconnected:
                        time.sleep(5)
                        reconnected = self._attempt_reconnect(i)
                except KeyError:
                    success = True
        return results
