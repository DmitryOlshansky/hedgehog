module main;

import std.array;
import std.algorithm;
import std.format;
import std.stdio;
import std.socket;
import std.exception;

import photon;

char[][string] hashmap;

enum ERROR = "ERROR\r\n";
enum CLIENT_ERROR = "CLIENT_ERROR %s\r\n";
enum SERVER_ERROR = "SERVER_ERROR %s\r\n";
enum STORED = "STORED\r\n";

// https://github.com/memcached/memcached/blob/master/doc/protocol.txt
void serverWorker(Socket client) {
    char[] buffer = new char[1024];
    char[] commandBuffer = [];
    for(;;) {
        auto size = client.receive(buffer);
        enforce(size >= 0);
        commandBuffer ~= buffer[0..size];
        char[][] pieces = [];
        if (commandBuffer.endsWith("\r\n")) {
            do {
                pieces = split(commandBuffer);
                writeln(commandBuffer);
                enforce(pieces.length >= 1);
                // <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
                // <data>\r\n
                auto command = pieces[0];
                auto key = pieces[1];
                auto flags = pieces[2];
                auto exptime = pieces[3];
                auto bytes = pieces[4];
                auto data = pieces[5];
                auto noreply = pieces.length >= 7 ? null : pieces[6];
                pieces = pieces[noreply == null ? 6 : 5 .. $];
                switch(command) {
                // Storage commands
                case "set":
                    hashmap[key.idup] = bytes;
                    commandBuffer = commandBuffer[pieces.map!(x => x.length).sum + 6 .. $];
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
                    enforce(false);
                }
            } while(pieces.length > 0);
            commandBuffer.length = 0;
            commandBuffer.assumeSafeAppend();
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
        spawn(() => serverWorker(client));
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
    version(Windows) {
        import core.memory;
        GC.disable(); // temporary for Win64 UMS threading
    }
    startloop();
    spawn(() => server());
    runFibers();
}
