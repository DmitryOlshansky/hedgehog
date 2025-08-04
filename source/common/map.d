module common.map;

import common.table;
import core.internal.spinlock;

private immutable size_t BUCKETS = 127;

size_t bucketOf(size_t hash) {
    return (hash >> 16) % BUCKETS;
}

size_t hashCode(T)(T data) {
    return data.hashOf;
}

size_t hashCode(scope const(ubyte)[] data) @nogc nothrow pure @safe
{
    size_t h = 0;
    foreach (b; data) {
        h = 31*h + b;
    }
    return h;
}

class Map(K, V) {
    struct Shard {
        Table!(K,V) map;
        SpinLock lock;
    }
    Shard[] shards;
    this() {
        shards = new Shard[BUCKETS];
        foreach (ref s; shards) {
            s.map = Table!(K,V)(32);
        }
    }

    V getOrDefault(K key, V default_) {
        auto h = hashCode(key);
        auto shard = &shards[bucketOf(h)];
        shard.lock.lock();
        scope(exit) shard.lock.unlock();
        auto v = shard.map.lookup(key, h);
        if (v) {
            (*v).acquire();
            return *v;
        }
        else {
            return default_;
        }
    }

    V put(K key, V value) {
        auto h = hashCode(key);
        auto shard = &shards[bucketOf(h)];
        shard.lock.lock();
        scope(exit) shard.lock.unlock();
        return shard.map.put(key, h, value);
    }

    bool opBinaryRight(string op:"in")(K key) {
        auto h = hashCode(key);
        auto shard = &shards[bucketOf(h)];
        shard.lock.lock();
        scope(exit) shard.lock.unlock();
        return shard.map.lookup(key, h) != null;
    }

    V remove(K key) {
        auto h = hashCode(key);
        auto shard = &shards[bucketOf(h)];
        shard.lock.lock();
        scope(exit) shard.lock.unlock();
        return shard.map.remove(key, h);
    }

    V removeIf(K key, scope bool delegate(ref V) cond) {
        auto h = hashCode(key);
        auto shard = &shards[bucketOf(h)];
        shard.lock.lock();
        scope(exit) shard.lock.unlock();
        auto value = shard.map.lookup(key, h);
        if (value != null && cond(*value)) {
            auto v = *value;
            shard.map.remove(key, h);
            return v;
        }
        else {
            return V.init;
        }
    }

    V cas(K key, V value, scope bool delegate(ref V) cond) {
        auto h = hashCode(key);
        auto shard = &shards[bucketOf(h)];
        shard.lock.lock();
        scope(exit) shard.lock.unlock();
        auto old = shard.map.lookup(key, h);
        if (old == null) return V.init;
        if (!cond(*old)) return value;
        auto oldValue = *old; // save as the next write occupies the same slot
        shard.map.put(key, h, value);
        return oldValue;
    }

    size_t length() {
        size_t len = 0;
        foreach (shard; shards) {
            len += shard.map.length;
        }
        return len;
    }
/*
    int opApply(scope int delegate(K, V) fn) {
        foreach (ref shard; shards) {
            shard.lock.lock();
            scope(exit) shard.lock.unlock();
            foreach (k, v; shard.map) {
                fn(k, v);
            }
        }
        return 1;
    }
*/
}

unittest {
    auto map = new Map!(int, string);
    map[0] = "hello";
    assert(map[0] == "hello");
    assert(map.length == 1);
    /*foreach (k, v; map) {
        assert(k == 0);
        assert(v == "hello");
    }
    assert(map.remove(0) == "hello");
    assert(!(0 in map));
    assert(map.length == 0);
    */
}

unittest {
     static struct A {
        int k;
     }
     int[A] hashmap;
     auto map = new Map!(A, int);
     map[A(32)] = 2;
     assert(map[A(32)] == 2);
}

unittest {
    auto map = new Map!(string, string);
    auto key = "hello, world!";
    foreach (i; 0..key.length) {
        map[key[0..i]] = "abc";
    }
    foreach (i; 0..key.length) {
        assert(map[key[0..i]] == "abc");
    }
}

unittest {
    auto map = new Map!(string, string);
    map["abc"] = "def";
    assert(map.removeIf("ABC", (ref string x) { return x == "def"; }) == null);
    assert(map["abc"] == "def");
    assert(map.removeIf("abc", (ref string x) { return x == "DEF"; }) == null);
    assert(map["abc"] == "def");
    assert(map.removeIf("abc", (ref string x) { return x == "def"; }) == "def");
    assert(("abc" !in map));
}

unittest {
    auto map = new Map!(string, int);
    assert(map.put("A", 1) == 0);
    assert(map.put("A", 2) == 1);
    assert(map.getOrDefault("A", 3) == 2);
    assert(map.getOrDefault("B", 3) == 3);
}
