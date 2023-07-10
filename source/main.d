module main;

import std.array, std.ascii, std.algorithm, std.format, std.stdio, std.socket, std.exception, std.regex;

import photon;

char[][string] hashmap;

enum ERROR = "ERROR\r\n";
enum CLIENT_ERROR = "CLIENT_ERROR %s\r\n";
enum SERVER_ERROR = "SERVER_ERROR %s\r\n";
enum STORED = "STORED\r\n";

void processCommand(Socket client) {
    char[] buffer = new char[1024];
    char[] commandBuffer = [];
    char[] readText(ref char[] buf) {
        for (int i=0; i<buf.length; i++){
            if (buf[i].isWhite()) {
                auto resp = buf[0..i];
                buf = buf[i..$];
                return resp;
            }
        }
        return null;
    }
    void skipWs(ref char[] buf) {
        for (int i=0; i<buf.length; i++){
            if (!buf[i].isWhite()) {
                buf = buf[i..$];
                break;
            }
        }
    }
    void skipNonWs(ref char[] buf) {
        for (int i=0; i<buf.length; i++) {
            if (buf[i].isWhite()) {
                buf = buf[i..$];
                break;
            }
        }
    }
    for(;;) {
        auto size = client.receive(buffer);
        enforce(size >= 0);
        commandBuffer ~= buffer[0..size];
        size_t pos = 0;
        skipWs(commandBuffer);
        // <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
        // <data>\r\n
        auto command = readText(commandBuffer);
        skipWs(commandBuffer);
        auto key = readText(commandBuffer);
        skipWs(commandBuffer);
        auto flags = readText(commandBuffer);
        skipWs(commandBuffer);
        skipNonWs(commandBuffer);
        skipWs(commandBuffer);
        auto bytes = readText(commandBuffer);
        skipWs(commandBuffer);
        auto data = readText(commandBuffer);
        skipWs(commandBuffer);
        const(char)[] noreply;
        if (noreply != null) {
            noreply = readText(commandBuffer);
            skipWs(commandBuffer);
        }
        writeln(">>>", commandBuffer);
        writeln(">>", command);
        switch(command) {
            // Storage commands
            case "set":
                hashmap[key.idup] = bytes;
                debug writeln(hashmap);
                if (noreply != null)
                    client.send(cast(ubyte[])STORED);
                break;
            case "add":
                break;
            case "replace":
                break;
            case "append":
                break;
            case "prepend":
                break;
            case "cas":
                break;
            // Retrieval commands
            case "get":
                auto ik = cast(immutable(char)[])key;
                auto val = hashmap[ik];
                client.send(format("VALUE %s %d %d\r\n", ik, 1, key.length));
                client.send(val);
                client.send("END\r\n");
                writefln("Key %s", ik);
                break;
            case "gets":
                break;
            case "gat":
                break;
            case "gats":
                break;
            // 
            default:
                break;
            }
    }
}

// https://github.com/memcached/memcached/blob/master/doc/protocol.txt
void serverWorker(Socket client) {
    processCommand(client);
}

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

void server() {
    Socket server = new TcpSocket();
    server.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, true);
    server.bind(new InternetAddress("0.0.0.0", 4321));
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

void main() {
    startloop();
    go(() => server());
    go(() => redisServer());
    runFibers();
}
