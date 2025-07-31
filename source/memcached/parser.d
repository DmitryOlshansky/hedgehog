module memcached.parser;
// https://github.com/memcached/memcached/blob/master/doc/protocol.txt

static import std.ascii;
import core.stdc.stdlib, core.stdc.string;
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
    // increment/decrement
    incr,
    decr,
    // retrival commands
    get,
    gets,
    // touch
    touch,
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
    case "incr":
        return Command.incr;
    case "decr":
        return Command.decr;
    case "get":
        return Command.get;
    case "gets":
        return Command.gets;
    case "touch":
        return Command.touch;
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
    EXPTIME_TOUCH_START,
    EXPTIME_TOUCH,
    EXPTIME_GATS_START,
    EXPTIME_GATS,
    START_CAS_UNIQUE,
    CAS_UNIQUE,
    SET_START_NOREPLY,
    SET_NOREPLY,
    START_NOREPLY_END,
    NOREPLY_END,
    START_VALUE,
    VALUE,
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

unittest {
    bool[256] table = computeIsWhiteTable();
    size_t cnt = 0;
    foreach (c; 0..256) {
        cnt += table[c];
    }
    assert(cnt == 4);
}

bool isWhite(ubyte c) { return isWhiteTable[c]; }

public struct Parser {
// parser state
    ubyte* buf;
    size_t bufLen;
    size_t bufCap;
    size_t pos;
    State state = State.START_COMMAND_NAME;
// parsed storage command variables
    Command command;
    ubyte[] key; // also used in delete command
    uint flags;
    long exptime;
    size_t bytes;
    long casUnqiue;
    bool noReply;
    ubyte[] data;
// incr/decr value
    ulong value;
// parsed get/gets command variables
    ubyte[]* keys;
    size_t keysLen;
    size_t keysCap;
    
    void feed(const(ubyte)[] slice) {
        if (bufCap < bufLen + slice.length) {
            if (bufCap == 0) bufCap = 2048;
            bufCap = max(bufLen + slice.length, bufCap * 2);
            buf = cast(ubyte*)realloc(buf, bufCap);
        }
        buf[bufLen .. bufLen + slice.length] = slice[];
        bufLen += slice.length;
    }

    bool skipWs() {
        for (size_t i=pos; i<bufLen; i++){
            if (!buf[i].isWhite()) {
                pos = i;
                return true;
            }
        }
        return false;
    }

    bool skipNonWs() {
        for (size_t i=pos; i<bufLen; i++) {
            if (buf[i].isWhite() || buf[i] == '\r') {
                pos = i;
                return true;
            }
        }
        return false;
    }

    bool parse() {
        if (state == State.END) {
            command = Command.none;
            key = null;
            keysLen = 0;
            flags = 0;
            exptime = 0;
            bytes = 0;
            casUnqiue = 0;
            noReply = false;
            data = null;
            size_t rem = bufLen - pos;
            memmove(buf, buf + pos, rem);
            bufLen = rem;
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
                if (command == Command.gat || command == Command.gats) {
                    state = EXPTIME_GATS_START;
                    goto case EXPTIME_GATS_START;
                }
                state = START_KEY;
                goto case;
            case START_KEY:
                if (!skipWs()) return false;
                state = KEY;
                goto case;
            case KEY:
                size_t start = pos;
                if (!skipNonWs()) return false;
                if (command == Command.get || command == Command.gets
                    || command == Command.gat || command == Command.gats) {
                    state = NEXT_KEY;
                    if (keysCap == keysLen) {
                        keysCap = keysCap + 10;
                        keys = cast(ubyte[]*)realloc(keys, keysCap * ubyte[].sizeof);
                    }
                    keys[keysLen++] = buf[start..pos];
                    goto case NEXT_KEY;
                }
                else if (command == Command.delete_) {
                    key = buf[start..pos];
                    state = START_NOREPLY_END;
                    goto case START_NOREPLY_END;
                }
                else if (command == Command.incr || command == Command.decr) {
                    key = buf[start..pos];
                    state = START_VALUE;
                    goto case START_VALUE;
                }
                else if (command == Command.touch) {
                    key = buf[start..pos];
                    state = EXPTIME_TOUCH_START;
                    goto case EXPTIME_TOUCH_START;
                }
                else {
                    key = buf[start..pos];
                    state = START_FLAGS;
                    goto case START_FLAGS;
                }
            case NEXT_KEY:
                if (!skipWs()) return false;
                if (buf[pos] == '\r') {
                    if (pos+1 == bufLen) return false;
                    enforce(buf[pos+1] == '\n');
                    pos += 2;
                    state = END;
                    goto case END;
                }
                state = KEY;
                goto case KEY;
            case EXPTIME_TOUCH_START:
                if (!skipWs()) return false;
                state = EXPTIME_TOUCH;
                goto case EXPTIME_TOUCH;
            case EXPTIME_TOUCH:
                size_t start = pos;
                if (!skipNonWs()) return false;
                exptime = to!long(cast(char[])buf[start..pos]);
                state = START_NOREPLY_END;
                goto case START_NOREPLY_END;
            case EXPTIME_GATS_START:
                if (!skipWs()) return false;
                state = EXPTIME_GATS;
                goto case EXPTIME_GATS;
            case EXPTIME_GATS:
                size_t start = pos;
                if (!skipNonWs()) return false;
                exptime = to!long(cast(char[])buf[start..pos]);
                state = START_KEY;
                goto case START_KEY;
            case START_FLAGS:
                if (!skipWs()) return false;
                state = FLAGS;
                goto case;
            case FLAGS:
                size_t start = pos;
                if (!skipNonWs()) return false;
                flags = to!uint(cast(char[])buf[start..pos]);
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
                    state = SET_START_NOREPLY;
                    goto case SET_START_NOREPLY;
                }
            case START_CAS_UNIQUE:
                if (!skipWs()) return false;
                state = CAS_UNIQUE;
                goto case;
            case CAS_UNIQUE:
                size_t start = pos;
                if (!skipNonWs()) return false;
                casUnqiue = to!long(cast(char[])buf[start..pos]);
                state = SET_START_NOREPLY;
                goto case SET_START_NOREPLY;
            case SET_START_NOREPLY:
                if (!skipWs()) return false;
                if (buf[pos] == '\r') {
                    if (pos+1 == bufLen) return false;
                    enforce(buf[pos+1] == '\n');
                    pos+=2;
                    noReply = false;
                    state = DATA;
                    goto case DATA;
                } else {
                    state = SET_NOREPLY;
                    goto case SET_NOREPLY;
                }
            case SET_NOREPLY:
                if (pos + 9 > bufLen) return false;
                enforce(cast(char[])buf[pos..pos+9] == "noreply\r\n");
                noReply = true;
                pos += 9;
                state = DATA;
                goto case DATA;
            case START_VALUE:
                if (!skipWs()) return false;
                state = VALUE;
                goto case;
            case VALUE:
                size_t start = pos;
                if(!skipNonWs()) return false;
                value = to!ulong(cast(char[])buf[start..pos]);
                state = START_NOREPLY_END;
                goto case START_NOREPLY_END;
            case START_NOREPLY_END:
                if (!skipWs()) return false;
                if (buf[pos] == '\r') {
                    if (pos+1 == bufLen) return false;
                    enforce(buf[pos+1] == '\n');
                    pos+=2;
                    noReply = false;
                    state = END;
                    goto case END;
                } else {
                    state = NOREPLY_END;
                    goto case NOREPLY_END;
                }
            case NOREPLY_END:
                if (pos + 9 > bufLen) return false;
                enforce(cast(char[])buf[pos..pos+9] == "noreply\r\n");
                noReply = true;
                pos += 9;
                state = END;
                goto case END;
            case DATA:
                if (pos + bytes + 2 > bufLen) return false;
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

    ~this() {
        free(keys);
        free(buf);
    }
}

version(unittest) {
    void testParse(T)(T[] command, scope void delegate(ref Parser)[] dgs...) {
        Parser parser;
        // feed all at once
        parser.feed(cast(const(ubyte)[])command);
        foreach (dg; dgs) {
            assert(parser.parse());
            dg(parser);
        }
        size_t i = 0;
        // feed by byte at a time
        foreach (dg; dgs) {
            while (!parser.parse()) {
                parser.feed(cast(const(ubyte)[])command[i..i+1]);
                i++;
            }
            dg(parser);
        }
    }
}


unittest {
    testParse(
        "add key 0 0 1\r\nA\r\nreplace key 0 1 1 noreply\r\nB\r\n"
        ~ "append key 1 1 1 noreply\r\nC\r\nprepend key 0 0 1\r\nD\r\n", (ref parser) {
        assert(parser.command == Command.add);
        assert(cast(string)parser.key == "key");
        assert(parser.flags == 0);
        assert(parser.exptime == 0);
        assert(!parser.noReply);
        assert(cast(string)parser.data == "A");
    }, (ref parser) {
        assert(parser.command == Command.replace);
        assert(cast(string)parser.key == "key");
        assert(parser.flags == 0);
        assert(parser.exptime == 1);
        assert(parser.noReply);
        assert(cast(string)parser.data == "B");
    }, (ref parser) {
        assert(parser.command == Command.append);
        assert(cast(string)parser.key == "key");
        assert(parser.flags == 1);
        assert(parser.exptime == 1);
        assert(parser.noReply);
        assert(cast(string)parser.data == "C");
    }, (ref parser) {
        assert(parser.command == Command.prepend);
        assert(cast(string)parser.key == "key");
        assert(parser.flags == 0);
        assert(parser.exptime == 0);
        assert(!parser.noReply);
        assert(cast(string)parser.data == "D");
    });
}

unittest {
    testParse("set some_key 0 0 10 noreply\r\nsome_value\r\ngets some_key\r\n", (ref parser) {
        assert(parser.command == Command.set);
        assert(cast(string)parser.key == "some_key");
        assert(parser.flags == 0);
        assert(parser.exptime == 0);
        assert(parser.bytes == 10);
        assert(parser.noReply == true);
        assert(cast(string)parser.data == "some_value");
    }, (ref parser) {
        assert(parser.command == Command.gets);
        assert(parser.keysLen == 1);
        assert(cast(string)parser.keys[0] == "some_key");
    });
}

unittest {
    testParse("get k1 k2\r\n", (ref parser) {
        assert(parser.keysLen == 2);
        assert(cast(string)parser.keys[0] == "k1");
        assert(cast(string)parser.keys[1] == "k2");
    });
}

unittest {
    testParse("set key 1 2 3\r\nval\r\n", (ref parser) {
        assert(parser.command == Command.set);
        assert(parser.flags == 1);
        assert(parser.exptime == 2);
        assert(cast(string)parser.data == "val");
    });
}

unittest {
    ubyte[] command = [115, 101, 116, 32, 97, 98, 99, 32, 49, 32, 49, 32, 50, 13, 10, 49, 50, 13, 10];
    testParse(command, (ref parser) {
    });
}

unittest {
    testParse("delete key\r\ndelete key2 noreply\r\n", (ref parser) {
        assert(parser.command == Command.delete_);
        assert(cast(string)parser.key == "key");
        assert(!parser.noReply);
    }, (ref parser) {
        assert(parser.command == Command.delete_);
        assert(cast(string)parser.key == "key2");
        assert(parser.noReply);
    });
}

unittest {
    testParse("incr key 2\r\nincr key2 3 noreply\r\ndecr key3 4000000000\r\ndecr key4 1 noreply\r\n", (ref parser) {
        assert(parser.command == Command.incr);
        assert(cast(string)parser.key == "key");
        assert(parser.value == 2);
        assert(!parser.noReply);
    }, (ref parser) {
        assert(parser.command == Command.incr);
        assert(cast(string)parser.key == "key2");
        assert(parser.value == 3);
        assert(parser.noReply);
    }, (ref parser) { 
        assert(parser.command == Command.decr);
        assert(cast(string)parser.key == "key3");
        assert(parser.value == 4_000_000_000);
        assert(!parser.noReply);
    }, (ref parser) {
        assert(parser.command == Command.decr);
        assert(cast(string)parser.key == "key4");
        assert(parser.value == 1);
        assert(parser.noReply);
    });
}

unittest {
    testParse("touch k 12 noreply\r\ntouch k2 23\r\n", (ref parser) {
        assert(parser.command == Command.touch);
        assert(cast(string)parser.key == "k");
        assert(parser.exptime == 12);
        assert(parser.noReply);
    }, (ref parser) {
        assert(parser.command == Command.touch);
        assert(cast(string)parser.key == "k2");
        assert(parser.exptime == 23);
        assert(!parser.noReply);
    });
}

unittest {
    testParse("gat 12 key1 key2\r\ngats 23 key3\r\n", (ref parser) {
        assert(parser.command == Command.gat);
        assert(cast(string[])(parser.keys[0..parser.keysLen]) == ["key1", "key2"]);
        assert(parser.exptime == 12);
    }, (ref parser) {
        assert(parser.command == Command.gats);
        assert(cast(string[])parser.keys[0..parser.keysLen] == ["key3"]);
        assert(parser.exptime == 23);
    });
}

unittest {
    testParse("cas key 0 1 5 321\r\nvalue\r\ncas key 0 1 5 123 noreply\r\nvalue\r\n", (ref parser) {
        assert(parser.command == Command.cas);
        assert(cast(string)parser.key == "key");
        assert(parser.flags == 0);
        assert(parser.exptime == 1);
        assert(cast(string)parser.data == "value");
        assert(!parser.noReply);
        assert(parser.casUnqiue == 321);
    }, (ref parser) {
        assert(parser.command == Command.cas);
        assert(cast(string)parser.key == "key");
        assert(parser.flags == 0);
        assert(parser.exptime == 1);
        assert(cast(string)parser.data == "value");
        assert(parser.noReply);
        assert(parser.casUnqiue == 123);
    });
}

unittest {
    Parser parser;
    parser.feed(cast(const(ubyte)[])"some ");
    try {
        parser.parse();
        assert(false);
    }
    catch (Exception e) {
        assert(e.message == "Bad command name " ~ "some");
    }
}