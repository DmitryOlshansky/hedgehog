#!/usr/bin/env python3
from pymemcache.client.base import Client
import threading

client = Client('127.0.0.1:11211')
client.delete('key')
client.set('key', '!')

def run_appends(c, n):
    for i in range(0, n):
        c.append('key', '0')

threads = []

for i in range(0, 10):
    c = Client("127.0.0.1:11211")
    threads.append(threading.Thread(target=run_appends, args=(c, 100,)))

for i in range(0, 10):
    threads[i].start()

for i in range(0, 10):
    threads[i].join()

assert(client.get('key')[1:] == b'0' * 1000)