#!/usr/bin/env python3
import socket

sock = socket.create_connection(("127.0.0.1", 11211))
# <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
# <data>\r\n
sock.send(b"set abc 1 1 2\r\n12\r\n")
data = sock.recv(1024)
print(data)
sock.send(b"get abc\r\n")
data = sock.recv(1024)
print(data)

