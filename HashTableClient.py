import socket
import json
import re
import http.client
import sys

class HashTableClient():
    def __init__(self):
        self.socket = None

    # connects to the service with the project name at
    # catalog.cse.nd.edu:9097
    def connect(self, project_name):
        # get json from nameserver
        h = http.client.HTTPConnection('catalog.cse.nd.edu:9097')
        h.request('GET', '/query.json')
        resp = h.getresponse().read()
        services = json.loads(resp)

        for service in services:
            if service["type"] == "hashtable" and service["project"] == project_name:
                self._connect_host(service["address"], service["port"])
                return
        print("ERROR: could not find server with provided name")
        sys.exit(1)

    # connects to the given host and port
    def _connect_host(self, host, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(5)
        self.socket.connect((host, port))

    #value should be a something that can be converted to json
    def insert(self, key, value):
        request = self._construct_request("insert", key=key, value=json.dumps(value))
        response = self._perform_request(request)

        if response["status"] == "success":
            return value
        elif response["status"] == "invalid" and response["exception"] == "KeyError":
            raise ValueError("Invalid Request - both key and value must exist and conform to interface guidelines")
        elif response["status"] == "invalid" and response["exception"] == "TypeError":
            raise TypeError("Invalid Request - the value sent was not valid json or the key was not a string.")
        elif response["status"] == "invalid" and response["exception"] == "ValueError":
            raise ValueError("Invalid Request - value must be json")
        else:
            raise ValueError("Invalid value")

    def lookup(self, key):
        request = self._construct_request("lookup", key=key)
        response = self._perform_request(request)

        if response["status"] == "success":
            return json.loads(response["result"])
        elif response["status"] == "failure":
            raise KeyError(response["err_msg"])
        else:
            raise ValueError(response["err_msg"])

    def scan(self, regex):
        request = self._construct_request("scan", regex=regex)
        response = self._perform_request(request)
        if response["status"] == "success":
            return [ (key, json.loads(val)) for (key, val) in json.loads(response["result"]) ]
        elif response["status"] == "invalid" and response["exception"] == "re.Error":
            raise re.error(response["err_msg"])
        elif response["status"] == "invalid" and response["exception"] == "ValueError":
            raise ValueError(response["err_msg"])
        else:
            raise KeyError(response["err_msg"])

    def remove(self, key):
        request = self._construct_request("remove", key=key)
        response = self._perform_request(request)
        if response["status"] == "success":
            return json.loads(response["result"])
        elif response["status"] == "failure":
            raise KeyError(response["err_msg"])
        else:
            raise ValueError(response["err_msg"])

    def close(self):
        self.socket.close()

    # key is a string
    # value is a json string
    # regex is a python regular expression
    def _construct_request(self, method, key=None, value=None, regex=None):
        request = {}
        request["method"] = method
        if key:
            request["key"] = key
        if value:
            request["value"] = value
        if regex:
            request["regex"] = regex
        request = json.dumps(request)
        return request

    # accepts a json string as a request object and
    # returns the servers response
    def _perform_request(self, request):
        # send the request
        request_size = len(request)
        self.socket.sendall(request_size.to_bytes(8, "big") + request.encode())
        #wait for the response
        response = b""
        resp_len = self.socket.recv(8)
        if not resp_len:
            raise socket.error()
        remainder = 8 - len(resp_len)
        while remainder > 0:
            val = self.socket.recv(remainder)
            if not val:
                raise socket.error()
            resp_len = resp_len + val
            remainder = 8 - len(resp_len)
        resp_len = int.from_bytes(resp_len, "big")
        remainder = resp_len
        while remainder > 0:
            val = self.socket.recv(min(remainder, 4096))
            if not val:
                raise socket.error()
            response = response + val
            remainder = resp_len - len(response)
        response = response.decode("ascii")
        return json.loads(response)
