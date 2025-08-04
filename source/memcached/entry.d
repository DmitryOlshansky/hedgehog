module memcached.entry;

import core.stdc.stdlib;
import std.traits;

enum State {
    ACTIVE,    // normal, inserted state
    INSERTING, // immune to deletion requests
    DELETING   // in the process of deletion
}

struct Entry {
    immutable(ubyte)[] key;   // to be able to delete from hashmap solely from Entry*
    immutable(ubyte)[] data;  // data associated with the key
    uint refCount;            // ref-counting
    uint flags;               // opaque flags set by client
    long expTime;             // expiration time unix timestamp in seconds
    immutable long casUnique; // protocol required stamp to support cas command
    State state;              // to protect from insert/delete races

    // intrusive linked lists
    Entry* next;              // general for temporary lists
    Entry* prev2, next2;      // for Sched
    Entry* prev3, next3;      // for LRU

    void acquire() {
        refCount++;
    }
}

Entry* allocate(uint refCount, immutable(ubyte)[] key, immutable(ubyte)[] data, uint flags, long expTime, long casUnique, State state) {
    Entry* e = cast(Entry*)malloc(Entry.sizeof);
    e.refCount = refCount;
    e.key = key;
    e.data = data;
    e.flags = flags;
    e.expTime = expTime;
    *cast(long*)&e.casUnique = casUnique;
    e.state = state;
    e.next = e.next2 = e.prev2 = e.next3 = e.prev3 = null;
    return e;
}

auto mallocedCopy(T)(T[] array) {
    Unqual!T* ptr = cast(Unqual!T*)malloc(T.sizeof * array.length);
    ptr[0..array.length] = array[];
    return cast(immutable)ptr[0..array.length];
}

T[] malloced(T)(size_t size) {
    T* ptr = cast(T*)malloc(T.sizeof * size);
    return ptr[0..size];
}

void release(Entry* e) {
    if (--e.refCount == 0) {
        free(cast(void*)e.key.ptr);
        free(cast(void*)e.data.ptr);
        free(e);
    }
}

