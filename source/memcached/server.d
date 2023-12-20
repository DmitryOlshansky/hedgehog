module memcached.server;

import memcached.parser, memcached.timer_queue;

import core.atomic;

import std.socket, std.stdio, std.exception, std.format;

import photon, rewind.map;

struct Entry {
    immutable(ubyte)[] key; // to be able to delete from hashmap solely from Entry*
    immutable(ubyte)[] data;
    immutable long expTime;
    immutable long casUnique;
    // intrusive linked list
    Entry* prev, next;
}

alias ObjectMap = Map!(immutable(ubyte)[], Entry*);

__gshared ObjectMap hashmap = new ObjectMap();
__gshared TimerQueue!(Entry, unixTime, (Entry* e) {
    debug writeln("Expired ", cast(string)e.key);
    hashmap.removeIf(e.key, (ref Entry* actual) {
        return actual == e;
    });
}) timerQueue;

shared long casUniqueCounter = 0;

long nextCasUnique() {
    return atomicFetchAdd(casUniqueCounter, 1);
}

long expirationTime(long expTime) {
    if (expTime <= 60*60*24*30) {
        return timerQueue.currentTime() + expTime;
    } else {
        return expTime;
    }
}

enum ERROR = "ERROR\r\n";
enum CLIENT_ERROR = "CLIENT_ERROR %s\r\n";
enum SERVER_ERROR = "SERVER_ERROR %s\r\n";
enum STORED = "STORED\r\n";

void processCommand(Socket client) {
    ubyte[] buffer = new ubyte[8096];
    Parser parser;
    for(;;) {
        auto size = client.receive(buffer);
        if (size == 0) break;
        enforce(size > 0);
        parser.feed(buffer[0..size]);
        while (parser.parse()) {
            auto cmd = parser.command;
            switch(cmd) with (Command) {
            case set:
                auto key = parser.key.idup;
                auto data = parser.data.idup;
                if (parser.exptime >= 0) {
                    auto expires = expirationTime(parser.exptime);
                    auto entry = new Entry(key, data, expires, nextCasUnique());
                    hashmap[key] = entry;
                    if (expires > 0) {
                        timerQueue.schedule(entry);
                    }
                    if (!parser.noReply) {
                        client.send(STORED);
                    }
                }
                break;
            case get:
                foreach (key; parser.keys) {
                    auto ik = cast(immutable ubyte[])key;
                    auto val = ik in hashmap;
                    if (val != null) {
                        client.send(format("VALUE %s %d %d\r\n", cast(string)ik, 1, (*val).data.length));
                        client.send((*val).data);
                        client.send("\r\n");
                    }
                }
                client.send("END\r\n");
                break;
            default:
                client.send(SERVER_ERROR.format("Unimplemented"));
            }
        }
    }
}

// https://github.com/memcached/memcached/blob/master/doc/protocol.txt
void serverWorker(Socket client) {
    processCommand(client);
}

void memcachedServer() {
    Socket server = new TcpSocket();
    server.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, true);
    server.bind(new InternetAddress("0.0.0.0", 11211));
    server.listen(1000);

    timerQueue.start();

    debug writeln("Started server");

    void processClient(Socket client) {
        go(() => serverWorker(client));
    }

    while(true) {
        try {
            debug writeln("Waiting for server.accept()");
            Socket client = server.accept();
            debug writeln("New client accepted");
            processClient(client);
        }
        catch(Exception e) {
            writefln("Failure to accept %s", e);
        }
    }
}