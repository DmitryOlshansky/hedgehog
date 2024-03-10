#!/usr/bin/env python3
from pymemcache.client.base import Client

client = Client('localhost:11211')
client.set('key', b'value')
result, cas_unique = client.gets('key')
assert(result == b'value')
cas_result = client.cas('key', b'value2', cas_unique)
assert(cas_result == True)
result, new_cas_unique = client.gets('key')
assert(result == b'value2')
cas_result = client.cas('key', 'value3', cas_unique) # stale cas
assert(cas_result == False)
client.delete('key')
cas_result = client.cas('key', 'value3', new_cas_unique) # deleted original
assert(cas_result == None)
result, new_cas_unique = client.gets('key')
assert(result == None)