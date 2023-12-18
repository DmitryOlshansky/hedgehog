module memcached.server;

import memcached.parser;

import std.socket, std.stdio, std.exception, std.format;

import photon, rewind.map;

alias ObjectMap = Map!(immutable(ubyte)[], immutable(ubyte)[]);

__gshared ObjectMap hashmap = new ObjectMap();
//immutable(ubyte)[][immutable(ubyte)[]] hashmap;

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
                hashmap[parser.key.idup] = parser.data.idup;
                if (!parser.noReply) {
                    client.send(STORED);
                }
                break;
            case get:
                foreach (key; parser.keys) {
                    auto ik = cast(immutable ubyte[])key;
                    debug writefln("Key %s", cast(string)ik);
                    auto val = ik in hashmap;
                    if (val != null) {
                        client.send(format("VALUE %s %d %d\r\n", cast(string)ik, 1, val.length));
                        client.send(*val);
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