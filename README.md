# Headgehog

Hedgehog is a simple drop-in replacement for memcached

## Building

Get D compiler and tools for your OS [here](https://dlang.org/download.html).
```
dub build
```

## Running

./headgehog <port-number> <memory-limit-in-gb>

## Under the hood

Headgehog is an example project that utilizes the power of photon transparent fiber scheduler to make synchronious I/O async.


