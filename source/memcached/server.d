module memcached.server;

import memcached.parser, memcached.timer_queue, memcached.entry, memcached.lru;

import core.atomic;

import std.socket, std.stdio, std.exception, std.format;

import photon, rewind.map;

alias ObjectMap = Map!(immutable(ubyte)[], Entry*);

__gshared ObjectMap hashmap = new ObjectMap();
__gshared TimerQueue!(Entry, unixTime, (Entry* e) {
    debug writeln("Expired ", cast(string)e.key);
    hashmap.removeIf(e.key, (ref Entry* actual) {
        return actual == e;
    });
}) timerQueue;
__gshared Lru lru;

shared long casUniqueCounter = 0;

long nextCasUnique() {
    return atomicFetchAdd(casUniqueCounter, 1);
}

long expirationTime(long expTime) {
    if (expTime <= 0) {
        return expTime;
    }
    else if (expTime <= 60*60*24*30) {
        return timerQueue.currentTime() + expTime;
    } else {
        return expTime;
    }
}

enum ERROR = "ERROR\r\n";
enum CLIENT_ERROR = "CLIENT_ERROR %s\r\n";
enum SERVER_ERROR = "SERVER_ERROR %s\r\n";
enum STORED = "STORED\r\n";

void processCommand(const ref Parser parser, Socket client) {
    auto cmd = parser.command;
    switch(cmd) with (Command) {
    case set:
        auto key = parser.key.idup;
        auto data = parser.data.idup;
        if (parser.exptime >= 0) {
            auto expires = expirationTime(parser.exptime);
            auto entry = new Entry(key, data, expires, nextCasUnique());
            auto old = hashmap.put(key, entry);
            if (expires > 0) {
                timerQueue.schedule(entry);
            }
            if (old && old.expTime > 0) {
                timerQueue.deschedule(old);
            }
            if (!parser.noReply) {
                client.send(STORED);
            }
        }
        break;
    case get:
        foreach (key; parser.keys) {
            auto ik = cast(immutable ubyte[])key;
            auto val = hashmap.getOrDefault(ik, null);
            if (val) {
                client.send(format("VALUE %s %d %d\r\n", cast(string)ik, 1, (*val).data.length));
                client.send((*val).data);
                client.send("\r\n");
            }
        }
        client.send("END\r\n");
        break;
    case delete_:
        auto val = hashmap.remove(cast(immutable ubyte[])parser.key);
        if (val != null) {
            if (val.expTime > 0) {
                timerQueue.deschedule(val);
            }
        }
        break;
    default:
        client.send(SERVER_ERROR.format("Unimplemented"));
    }
}

void serverWorker(Socket client) {
    ubyte[] buffer = new ubyte[8096];
    Parser parser;
    try {
        for(;;) {
            auto size = client.receive(buffer);
            if (size == 0) {
                client.close();
                break;
            }
            enforce(size > 0);
            parser.feed(buffer[0..size]);
            while (parser.parse())
                processCommand(parser, client);
        }
    }
    catch (Exception e) {
        writeln(e);
        client.close();
    }
}

void memcachedServer(size_t maxSize, ushort port, int backlog) {
    lru = Lru(maxSize);
    Socket server = new TcpSocket();
    server.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, true);
    server.bind(new InternetAddress("0.0.0.0", port));
    server.listen(backlog);

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