#!/usr/bin/env python3
from pymemcache.client.base import Client

client = Client('127.0.0.1:11211')
client.delete('key')
client.append('key', 'value')
assert(client.get('key') == None)
client.prepend('key', 'value')
assert(client.get('key') == None)

client.set('key', 'value')

client.append('key', '2')
assert(client.get('key') == b'value2')
client.prepend('key', '1')
assert(client.get('key') == b'1value2')
