module memcached.entry;

enum State {
    ACTIVE,    // normal, inserted state
    INSERTING, // immune to deletion requests
    DELETING   // in the process of deletion
}

struct Entry {
    immutable(ubyte)[] key;   // to be able to delete from hashmap solely from Entry*
    immutable(ubyte)[] data;  // data associated with the key
    uint flags;               // opaque flags set by client
    long expTime;             // expiration time unix timestamp in seconds
    immutable long casUnique; // protocol required stamp to support cas command
    State state;              // to protect from insert/delete races

    // intrusive linked lists
    Entry* next;              // general for temporary lists
    Entry* prev2, next2;      // for Sched
    Entry* prev3, next3;      // for LRU
}

