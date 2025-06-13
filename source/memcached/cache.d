module memcached.cache;

import memcached.entry, memcached.lru, memcached.sched;
import core.atomic;
import common.map;

enum CasResult {
    notFound,
    exists,
    updated
}

debug import std.stdio;
private:

// set DELETING state if not already, true if we are the first to start deletion
bool startDeletion(Entry* entry) {
    return cas(&entry.state, State.ACTIVE, State.DELETING);
}

shared long casUniqueCounter = 0;

long nextCasUnique() {
    return atomicFetchAdd(casUniqueCounter, 1);
}

alias KV = Map!(immutable(ubyte)[], Entry*);

__gshared KV kv = new KV();

alias SCHED = Sched!(Entry, unixTime, (Entry* e) {
    debug writeln("Expired ", cast(string)e.key);
    deleteFromKvAndLru(e);
});

__gshared SCHED sched;

alias LRU = Lru!(Entry, x => x.sizeof + x.key.length + x.data.length);
__gshared LRU lru;

void deleteFromKvAndLru(Entry* e) {
    if (startDeletion(e)) {
        kv.removeIf(e.key, (ref Entry* actual) {
            return actual == e;
        });
        lru.purge(e);
    }
}

void deleteFromKvAndSched(Entry* e) {
    if (startDeletion(e)) {
        kv.removeIf(e.key, (ref Entry* actual) {
            return actual == e;
        });
        if (e.expTime > 0) {
            sched.deschedule(e);
        }
    }
}

void deleteFromLruAndSched(Entry* e) {
    if (startDeletion(e)) {
        lru.purge(e);
        if (e.expTime > 0) {
            sched.deschedule(e);
        }
    }
}

void purgeAll(Entry* e) {
    while (e != null) {
        deleteFromKvAndSched(e);
        e = e.next;
    }
}

public:

void cacheInit(size_t size) {
    lru = LRU(size);
    sched.start();
}

long cacheCurrentTime() {
    return sched.currentTime();
}

Entry* cacheGet(immutable(ubyte)[] key) {
    Entry* e = kv.getOrDefault(key, null);
    if (e != null) {
        lru.refresh(e);
    }
    return e;
}

Entry* cacheGat(immutable(ubyte)[] key, long expires) {
    Entry* e = kv.getOrDefault(key, null);
    if (e != null) {
        lru.refresh(e);
        sched.refresh(e, expires);
    }
    return e;
}

void cacheSet(immutable(ubyte)[] key, immutable(ubyte)[] data, uint flags, long expires) {
    auto entry = new Entry(key, data, flags, expires, nextCasUnique(), State.INSERTING);
    auto old = kv.put(key, entry);
    auto toPurge = lru.insert(entry);
    purgeAll(toPurge);
    if (expires > 0) {
        sched.schedule(entry);
    }
    // new entry is stored everywhere, now it can be deleted
    atomicStore(entry.state, State.ACTIVE);
    if (old != null) {
        deleteFromLruAndSched(old);
    }
}

bool cacheDelete(immutable(ubyte)[] key) {
    Entry* e = kv.remove(key);
    if (e != null) {
        deleteFromLruAndSched(e);
    }
    return e != null;
}

CasResult cacheCas(immutable(ubyte)[] key, immutable(ubyte)[] data, uint flags, long expires, long casUnqiue) {
    auto entry = new Entry(key, data, flags, expires, nextCasUnique(), State.INSERTING);
    auto result = kv.cas(key, entry, (ref Entry* e){
        return e.casUnique == casUnqiue;
    });
    if (result == null) return CasResult.notFound;
    if (result.casUnique == entry.casUnique) return CasResult.exists;
    auto toPurge = lru.insert(entry);
    purgeAll(toPurge);
    if (expires > 0) {
        sched.schedule(entry);
    }
    atomicStore(entry.state, State.ACTIVE);
    deleteFromLruAndSched(result);
    return CasResult.updated;
}

