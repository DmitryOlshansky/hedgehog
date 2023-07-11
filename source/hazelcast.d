module hazelcast;

import std.socket, std.stdio;

import photon;


// https://github.com/memcached/memcached/blob/master/doc/protocol.txt
void serverWorker(Socket client) {
    ubyte[8096] storage;
    ptrdiff_t amount = client.receive(storage[]);
    ubyte[] buf = storage[0..amount];
}

void hazelcastServer() {
    Socket server = new TcpSocket();
    server.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, true);
    server.bind(new InternetAddress("0.0.0.0", 5701));
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