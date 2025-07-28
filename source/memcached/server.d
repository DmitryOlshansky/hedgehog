module memcached.server;

import memcached.parser, memcached.cache;

import core.atomic;

import std.socket, std.stdio, std.exception, std.format;

import photon;

import core.stdc.string : strlen, strerror;
import core.stdc.errno;

string errnoString() {    
    auto s = strerror(errno);
    return s[0..s.strlen].idup;
}

long expirationTime(long expTime) {
    if (expTime <= 0) {
        return expTime;
    }
    else if (expTime <= 60*60*24*30) {
        return cacheCurrentTime() + expTime;
    } else {
        return expTime;
    }
}

enum ERROR = "ERROR\r\n";
enum CLIENT_ERROR = "CLIENT_ERROR %s\r\n";
enum SERVER_ERROR = "SERVER_ERROR %s\r\n";
enum STORED = "STORED\r\n";
enum NOT_STORED = "NOT_STORED\r\n";
enum EXISTS = "EXISTS\r\n";
enum NOT_FOUND = "NOT_FOUND\r\n";

void processModify(alias modify)(
    const ref Parser parser,
    Socket client
) {
    if (parser.exptime >= 0) {
        auto ik = parser.key.idup;
        auto data = parser.data.idup;
        auto flags = parser.flags;
        auto expires = expirationTime(parser.exptime);
        for (;;) {
            auto val = cacheGet(ik);
            if (!val) {
                if (!parser.noReply) client.send(NOT_FOUND);
                break;
            }
            auto toInsert = modify(val.data, data);
            auto casUnique = val.casUnique;
            auto result = cacheCas(ik, toInsert, flags, expires, casUnique);
            if (result == CasResult.updated) {
                if(!parser.noReply) client.send(STORED);
                break;
            }
        }
    }
}

struct iovec {
    void* iov_base;
    size_t iov_len;
}

extern(C) ssize_t writev(int fd, const iovec *vector, int count);


void sendv(T...)(int fd, T bufs) {
    iovec[bufs.length] vecs;
    foreach (i, buf; bufs) {
        vecs[i].iov_base = cast(void*)buf.ptr;
        vecs[i].iov_len = buf.length;
    }
    writev(fd, vecs.ptr, bufs.length);
}

void processCommand(const ref Parser parser, Socket client) {
    auto cmd = parser.command;
    switch(cmd) with (Command) {
    case set:
        if (parser.exptime >= 0) {
            auto key = parser.key.idup;
            auto data = parser.data.idup;
            auto flags = parser.flags;
            auto expires = expirationTime(parser.exptime);
            cacheSet(key, data, flags, expires);
            if (!parser.noReply) {
                client.send(STORED);
            }
        }
        break;
    case get:
        foreach (key; parser.keys) {
            auto ik = cast(immutable ubyte[])key;
            auto val = cacheGet(ik);
            if (val) {
                sendv(client.handle, 
                    format("VALUE %s %d %d\r\n", cast(string)ik, 1, (*val).data.length),
                    (*val).data,
                    "\r\n"
                );
            }
        }
        client.send("END\r\n");
        break;
    case gets:
        foreach (key; parser.keys) {
            auto ik = cast(immutable ubyte[])key;
            auto val = cacheGet(ik);
            if (val) {
                sendv(client.handle, 
                    format("VALUE %s %d %d %d\r\n", cast(string)ik, 1, (*val).data.length, (*val).casUnique),
                    (*val).data,
                    "\r\n"
                );
            }
        }
        client.send("END\r\n");
        break;
    case gat:
        foreach (key; parser.keys) {
            auto ik = cast(immutable ubyte[])key;
            auto expires = expirationTime(parser.exptime);
            auto val = cacheGat(ik, expires);
            if (val) {
                sendv(client.handle, 
                    format("VALUE %s %d %d\r\n", cast(string)ik, 1, (*val).data.length),
                    (*val).data,
                    "\r\n"
                );
            }
        }
        client.send("END\r\n");
        break;
    case gats:
        foreach (key; parser.keys) {
            auto ik = cast(immutable ubyte[])key;
            auto expires = expirationTime(parser.exptime);
            auto val = cacheGat(ik, expires);
            if (val) {
                sendv(client.handle, 
                    format("VALUE %s %d %d %d\r\n", cast(string)ik, 1, (*val).data.length, (*val).casUnique),
                    (*val).data,
                    "\r\n"
                );
            }
        }
        client.send("END\r\n");
        break;
    case cas:
        if (parser.exptime >= 0) {
            auto ik = parser.key.idup;
            auto data = parser.data.idup;
            auto flags = parser.flags;
            auto expires = expirationTime(parser.exptime);
            auto val = cacheCas(ik, data, flags, expires, parser.casUnqiue);
            if (!parser.noReply) {
                final switch (val) {
                    case CasResult.notFound:
                        client.send(NOT_FOUND);
                        break;
                    case CasResult.exists:
                        client.send(EXISTS);
                        break;
                    case CasResult.updated:
                        client.send(STORED);
                        break;
                }
            }
        }
        break;
    case delete_:
        auto resp = cacheDelete(cast(immutable ubyte[])parser.key);
        if (!parser.noReply) {
            if(resp) {
                client.send(STORED);
            } else {
                client.send(NOT_FOUND);
            }
        }
        break;
    case append:
        processModify!((a,b) => a ~ b)(parser, client);
        break;
    case prepend:
        processModify!((a,b) => b ~ a)(parser, client);
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
            if(size < 0) {
                throw new Exception("Error on connection " ~ errnoString());
            }
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
    Socket server = new TcpSocket();
    server.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, true);
    server.bind(new InternetAddress("0.0.0.0", port));
    server.listen(backlog);

    cacheInit(maxSize);

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