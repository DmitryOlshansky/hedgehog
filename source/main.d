module main;

import std.getopt, std.conv, std.algorithm;

import photon, memcached.server, redis;

void main(string[] argv) {
    ushort port = 11211;
    string memory = "1024m";
    getopt(argv,
        "p", &port,
        "m", &memory
    );
    size_t memorySize;
    if (memory.endsWith("m")) {
        memorySize = to!size_t(memory[0..$-1]) * 1024 * 1024;
    }
    else if(memory.endsWith("g")) {
        memorySize = to!size_t(memory[0..$-1]) * 1024 * 1024 * 1024;
    }
    else {
        memorySize = to!size_t(memory);
    }
    startloop();
    go(() => memcachedServer(memorySize, port, 1000));
    // go(() => redisServer());
    runFibers();
}
