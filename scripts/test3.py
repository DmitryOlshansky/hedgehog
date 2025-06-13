#!/usr/bin/env python3
from pymemcache.client.base import Client
import time

client = Client('127.0.0.1:11211')
client.set('some_key', 'some_value', 1)
time.sleep(2)
result = client.get('some_key')
assert result == None
