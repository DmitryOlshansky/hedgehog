module main;

import std.stdio;
import std.socket;

import photon;

void server_worker(Socket client) {
    //scope processor =  new HelloWorldProcessor(client);
    //processor.run();
}

void server() {
    Socket server = new TcpSocket();
    server.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, true);
    server.bind(new InternetAddress("0.0.0.0", 11211));
    server.listen(1000);

    debug writeln("Started server");

    void processClient(Socket client) {
        spawn(() => server_worker(client));
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
