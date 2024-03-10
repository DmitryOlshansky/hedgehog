#!/usr/bin/env python3
from pymemcache.client.base import Client

client = Client('localhost:11211')
client.set('some_key', 'some_value')
result = client.get('some_key')
assert(result == b'some_value')
client.delete('some_key')
result = client.get('some_key')
assert(result == None)

