# lots of help from https://docs.python.org/3/howto/sockets.html
import sys
import time
import socket
import json
import HashTable
import select
import os
import threading
import random

# returns True if the connection is expected to continue
# returns False if the connection has been closed
def handle_request(cs, table):
    response = {}
    # get the request and parse it into a dictionary
    try:
        request = parse_request(cs)
    except ConnectionResetError:
        print("Connection reset with client.")
        return False
    except MemoryError:
        response["status"] = "invalid"
        response["err_msg"] = "Maximum message size is 1 GB. You need to either reduce your message size or ensure that your sending the correct message length as an 8 byte integer initially. Connection Terminated."
        response["exception"] = "MemoryError"
        response = json.dumps(response)
        resp_size = len(response)
        cs.sendall(resp_size.to_bytes(8, "big") + response.encode())
        cs.close()
        return False
    except:
        response["status"] = "invalid"
        response["err_msg"] = "Incorrectly formatted message. Connection terminated."
        response["exception"] = "RequestError"
        response = json.dumps(response)
        resp_size = len(response)
        cs.sendall(resp_size.to_bytes(8, "big") + response.encode())
        cs.close()
        return False

    if not request:
        cs.close() # may cause problems, come back to this later
        return False

    try:
        request["method"]
    except:
        response["status"] = "invalid"
        response["err_msg"] = "Incorrectly formatted message"
        response["exception"] = "RequestError"
        response = json.dumps(response)
        resp_size = len(response)
        cs.sendall(resp_size.to_bytes(8, "big") + response.encode())
        cs.close()
        return False
    if request["method"] == "insert":
        # print("INSERT REQUEST")
        try:
            # check if the value is valid json
            json.loads(request["value"])
            if not isinstance(request["key"], str):
                raise TypeError()
            table.insert(request["key"], request["value"])
            response["status"] = "success"
        except KeyError:
            response["status"] = "invalid"
            response["err_msg"] = "Need to include both key and value for insert request"
            response["exception"] = "KeyError"
        except TypeError:
            response["status"] = "invalid"
            response["err_msg"] = "Need to ensure the value is a valid json object and key is a string"
            response["exception"] = "TypeError"
        except json.JSONDecodeError:
            response["status"] = "invalid"
            response["err_msg"] = "Need to ensure the value is a valid json object"
            response["exception"] = "ValueError"
    elif request["method"] == "remove":
        try:
            result = table.remove(request["key"])
            # print("REMOVE REQUEST", len(table.table.keys()))
            if result:
                response["status"] = "success"
                response["result"] = result
            else:
                response["status"] = "failure"
                response["err_msg"] = "Key does not exist"
                response["exception"] = "KeyError"
        except KeyError:
            response["status"] = "invalid"
            response["err_msg"] = "Invalid request - the request does not include the key field"
            response["exception"] = "ValueError"

    elif request["method"] == "scan":
        # print("SCAN REQUEST")
        try:
            result = table.scan(request["regex"])
            # decide later if we need to check if result has length greater than zero
            if result == "Re.error":
                response["status"] = "invalid"
                response["err_msg"] = "Invalid Regex"
                response["exception"] = "re.Error"
            elif result:
                response["status"] = "success"
                response["result"] = json.dumps(result)
            else:
                response["status"] = "success"
                response["result"] = json.dumps([])
        except KeyError:
            response["status"] = "invalid"
            response["exception"] = "ValueError"
            response["err_msg"] = "Invalid request - regex must exist"

    elif request["method"] == "lookup":
        # print("LOOKUP REQUEST")
        try:
            result = table.lookup(request["key"])
            if result:
                response["status"] = "success"
                response["result"] = result
            else:
                response["status"] = "failure"
                response["exception"] = "KeyError"
                response["err_msg"] = "No such key found in table"
        except KeyError:
            response["status"] = "invalid"
            response["exception"] = "ValueError"
            response["err_msg"] = "The key field in the request does not exist"
    else:
        response["status"] = "invalid"
        response["exception"] = "MethodError"
        response["err_msg"] = "Invalid method call"

    response = json.dumps(response)
    resp_size = len(response)
    cs.sendall(resp_size.to_bytes(8, "big") + response.encode())
    return True


def parse_request(cs):
    # get the message length from the first 8 bytes
    data_len = cs.recv(8)
    if not data_len:
        return None
    remainder = 8 - len(data_len)
    while remainder > 0:
        new_data = cs.recv(min(remainder, 4096))
        if not new_data:
            return None
        data_len = data_len + new_data
        remainder = 8 - len(data_len)
    length = int.from_bytes(data_len, "big")
    # we are going to cap the max value size at 1GB
    if length > 1000000000:
        raise MemoryError("Message too long")
    remainder = length
    data = b""
    while remainder > 0:
        data = data + cs.recv(min(remainder, 4096))
        remainder = length - len(data)
    data = data.decode("ascii")
    return json.loads(data)

def update_name_server(port, project_name):
    name_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while True:
        message = {}
        message["type"] = "hashtable"
        message["owner"] = os.environ["USER"]
        message["port"] = port
        message["project"] = project_name

        name_socket.sendto(json.dumps(message).encode(), ("catalog.cse.nd.edu", 9097))
        # print("Updated name server.")
        time.sleep(60)

def main():
    if not sys.argv[1]:
        print("Usage: ./HashTableServer.py SERVICENAME")
        sys.exit(1)

    NAME = sys.argv[1]

    if len(sys.argv) > 2:
        PORT = int(sys.argv[2])
    else:
        PORT = 0

    clients = {}

    # instantiate HashTable
    table = HashTable.HashTable("table.txn", "table.ckpt")

    # setup tcp socket
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket.setdefaulttimeout(600)
    master_socket.bind((socket.gethostname(), PORT))
    master_socket.listen(5)
    print("Listening on", master_socket.getsockname()[1], "...")
    PORT = master_socket.getsockname()[1]

    # we will create a separate thread to send updates to the name server
    name_server_thread = threading.Thread(target=update_name_server,args=(PORT,NAME))
    name_server_thread.daemon = True
    name_server_thread.start()

    sockets = { master_socket }

    # help from http://pymotw.com/2/select/
    while True:
        # get readable sockets
        read, write, err = select.select(sockets, [], [])

        # loop through each readable socket
        random.shuffle(read)
        for sock in read:
            # if its the master socket, then accept the incoming connection
            if sock == master_socket:
                (cs, address) = master_socket.accept()
                clients[cs] = address
                # CONSIDER SETBLOCKING(0) *******
                sockets.add(cs)
                print("Accept request from", address)
            else: # if not, then its a client
                # print("Handling request from", clients[sock])
                # if handle request returns false, then we have disconnected
                # from the client for some reason or another
                if not handle_request(sock, table):
                    print("client dc:", clients[sock])
                    del clients[sock]
                    sockets.remove(sock)

if __name__=="__main__":
    main()
