module redis;

import std.socket, std.stdio, std.regex;

import photon;

enum RedisProto {
    String = '+',
    Error = '-',
    Integer = ':',
    BulkString = '$',
    Array ='*'
}

enum terminator = "\r\n";

unittest {
    auto s = "+OK\r\n";
    assert(s[0] == RedisProto.String);
}

void redisWorker(Socket client) {
    char[8096] buf;
    auto len = client.receive(buf[]);
    auto cmd = buf[0..len];
    auto m = matchFirst(cmd, `HELLO \d+`);
    writeln(m);
}

void redisServer() {
    Socket server = new TcpSocket();
    server.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, true);
    server.bind(new InternetAddress("0.0.0.0", 6379));
    server.listen(1000);

    debug writeln("Started Redis server");

    void processClient(Socket client) {
        go(() => redisWorker(client));
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


