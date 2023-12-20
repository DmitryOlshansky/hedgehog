module memcached.parser;

static import std.ascii;
import std.algorithm, std.conv, std.exception;

private:

public enum MAX_KEY_LENGTH = 250;

public enum Command {
    none, // initial value indicating invalid command
    // storage commands
    set,
    add,
    replace,
    append,
    prepend,
    cas,
    // retrival commands
    get,
    gets,
    // get and touch
    gat,
    gats,
    // deletion
    delete_
}

Command commandFromString(const(char)[] commandName) {
    switch (commandName) {
    case "set":
        return Command.set;
    case "add":
        return Command.add;
    case "replace":
        return Command.replace;
    case "append":
        return Command.append;
    case "prepend":
        return Command.prepend;
    case "cas":
        return Command.cas;
    case "get":
        return Command.get;
    case "gets":
        return Command.gets;
    case "gat":
        return Command.gat;
    case "gats":
        return Command.gats;
    case "delete":
        return Command.delete_;
    default:
        enforce(false, "Bad command name " ~ commandName);
        assert(0);
    }
}

// Storage commands:
//
// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
// <data>\r\n
// cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
// <data>\r\n
//
// Retrival command:
// get <key>*\r\n
// gets <key>*\r\n
enum State {
    START_COMMAND_NAME,
    COMMAND_NAME,
    START_KEY,
    KEY,
    NEXT_KEY, // after first key in retrival commands
    START_FLAGS,
    FLAGS,
    START_EXPTIME,
    EXPTIME,
    START_BYTES,
    BYTES,
    START_CAS_UNIQUE,
    CAS_UNIQUE,
    START_NOREPLY,
    NOREPLY,
    DATA,
    END
}

bool[256] isWhiteTable = computeIsWhiteTable();

bool[256] computeIsWhiteTable() {
    bool[256] table;
    foreach (c; 0..128) {
        // from the protocol perspective '\r' and '\n' are distinct values not white space
        table[c] = std.ascii.isWhite(c) && c != '\r' && c !='\n';
    }
    return table;
}

bool isWhite(ubyte c) { return isWhiteTable[c]; }

public struct Parser {
// parser state
    ubyte[] buf;
    size_t pos;
    State state = State.START_COMMAND_NAME;
// parsed storage command variables
    Command command;
    ubyte[] key; // also used in delete command
    ushort flags;
    long exptime;
    size_t bytes;
    long casUnqiue;
    bool noReply;
    ubyte[] data;
// parsed get/gets command variables
    ubyte[][] keys;
    
    void feed(const(ubyte)[] slice) {
        buf ~= slice;
    }

    bool skipWs() {
        for (size_t i=pos; i<buf.length; i++){
            if (!buf[i].isWhite()) {
                pos = i;
                return true;
            }
        }
        return false;
    }

    bool skipNonWs() {
        for (size_t i=pos; i<buf.length; i++) {
            if (buf[i].isWhite() || buf[i] == '\r') {
                pos = i;
                return true;
            }
        }
        return false;
    }

    bool parse() {
        import std.stdio;
        if (state == State.END) {
            command = Command.none;
            key = null;
            keys = keys[0..0];
            keys.assumeSafeAppend();
            flags = 0;
            exptime = 0;
            bytes = 0;
            casUnqiue = 0;
            noReply = false;
            data = null;
            size_t rem = buf.length - pos;
            copy(buf[pos..$], buf[0..rem]);
            buf = buf[0..rem];
            buf.assumeSafeAppend();
            pos = 0;
            state = State.START_COMMAND_NAME;
        }
        for (;;) {
            final switch(state) with (State) {
            case START_COMMAND_NAME:           
                if (!skipWs()) return false;
                state = COMMAND_NAME;
                goto case;
            case COMMAND_NAME:
                size_t start = pos;
                if (!skipNonWs()) return false;
                command = commandFromString(cast(char[])buf[start..pos]);
                state = START_KEY;
                goto case;
            case START_KEY:
                if (!skipWs()) return false;
                state = KEY;
                goto case;
            case KEY:
                size_t start = pos;
                if (!skipNonWs()) return false;
                if (command == Command.get || command == Command.gets) {
                    state = NEXT_KEY;
                    keys ~= buf[start..pos];
                    goto case NEXT_KEY;
                }
                else {
                    key = buf[start..pos];
                    state = START_FLAGS;
                    goto case START_FLAGS;
                }
            case NEXT_KEY:
                if (!skipWs()) return false;
                if (buf[pos] == '\r') {
                    if (pos+1 == buf.length) return false;
                    enforce(buf[pos+1] == '\n');
                    pos += 2;
                    state = END;
                    goto case END;
                }
                state = KEY;
                goto case KEY;
            case START_FLAGS:
                if (!skipWs()) return false;
                state = FLAGS;
                goto case;
            case FLAGS:
                size_t start = pos;
                if (!skipNonWs()) return false;
                flags = to!ushort(cast(char[])buf[start..pos]);
                state = START_EXPTIME;
                goto case;
            case START_EXPTIME:
                if (!skipWs()) return false;
                state = EXPTIME;
                goto case;
            case EXPTIME:
                size_t start = pos;
                if (!skipNonWs()) return false;
                exptime = to!long(cast(char[])buf[start..pos]);
                state = START_BYTES;
                goto case;
            case START_BYTES:
                if (!skipWs()) return false;
                state = BYTES;
                goto case;
            case BYTES:
                size_t start = pos;
                if (!skipNonWs()) return false;
                bytes = to!size_t(cast(char[])buf[start..pos]);
                if (command == Command.cas) {
                    state = START_CAS_UNIQUE;
                    goto case START_CAS_UNIQUE;
                }
                else {
                    state = START_NOREPLY;
                    goto case START_NOREPLY;
                }
            case START_CAS_UNIQUE:
                if (!skipWs()) return false;
                state = CAS_UNIQUE;
                goto case;
            case CAS_UNIQUE:
                size_t start = pos;
                if (!skipNonWs()) return false;
                casUnqiue = to!long(cast(char[])buf[start..pos]);
                goto case;
            case START_NOREPLY:
                if (!skipWs()) return false;
                if (buf[pos] == '\r') {
                    if (pos+1 == buf.length) return false;
                    enforce(buf[pos+1] == '\n');
                    pos+=2;
                    noReply = false;
                    state = DATA;
                    goto case DATA;
                } else {
                    state = NOREPLY;
                    goto case NOREPLY;
                }
            case NOREPLY:
                if (pos + 9 > buf.length) return false;
                enforce(cast(char[])buf[pos..pos+9] == "noreply\r\n");
                noReply = true;
                pos += 9;
                state = DATA;
                goto case;
            case DATA:
                if (pos + bytes + 2 > buf.length) return false;
                data = buf[pos..pos+bytes];
                pos += bytes;
                state = END;
                enforce(cast(char[])buf[pos..pos+2] == "\r\n");
                pos += 2;
                goto case;
            case END:
                return true;
            }
        }
        assert(0);
    }
}


unittest {
    string command = "set some_key 0 0 10 noreply\r\nsome_value\r\nget some_key\r\n";
    Parser parser;
    parser.feed(cast(const(ubyte)[])command);
    assert(parser.parse());
    assert(parser.command == Command.set);
    assert(cast(string)parser.key == "some_key");
    assert(parser.flags == 0);
    assert(parser.exptime == 0);
    assert(parser.bytes == 10);
    assert(parser.noReply == true);
    assert(cast(string)parser.data == "some_value");

    assert(parser.parse());
    assert(parser.command == Command.get);
    assert(parser.keys.length == 1);
    assert(cast(string)parser.keys[0] == "some_key");
}

unittest {
    string command = "set key 1 2 3\r\nval\r\n";
    Parser parser;
    parser.feed(cast(const(ubyte)[])command);
    assert(parser.parse());
    assert(parser.command == Command.set);
    assert(parser.flags == 1);
    assert(parser.exptime == 2);
    assert(cast(string)parser.data == "val");
}

unittest {
    string command = "set key 1 2 3\r\nval\r\n";
    Parser parser;
    size_t i = 0;
    while (!parser.parse()) {
        parser.feed(cast(const(ubyte)[])command[i..i+1]);
        i++;
    }
    assert(cast(string)parser.key == "key");
    assert(parser.flags == 1);
    assert(parser.exptime == 2);
    assert(cast(string)parser.data == "val");
}

unittest {
    ubyte[] buf = [115, 101, 116, 32, 97, 98, 99, 32, 49, 32, 49, 32, 50, 13, 10, 49, 50, 13, 10];
    Parser parser;
    parser.feed(buf);
    assert(parser.parse());
}
