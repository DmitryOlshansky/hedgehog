module main;

import std.array, std.ascii, std.algorithm, std.format, std.stdio, std.socket, std.exception, std.regex;

import photon, memcached.server, redis;

void main() {
    startloop();
    go(() => memcachedServer());
    // go(() => redisServer());
    runFibers();
}
