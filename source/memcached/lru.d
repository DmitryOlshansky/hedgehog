module memcached.lru;

import common.intrusive_queue;

import core.internal.spinlock, core.atomic;

struct Lru(T, alias sizeOf) {
    size_t availMemory;
    SpinLock lock;
    T* purgeQueue;

    // insert new entry purging existing entries to make space for it
    // returns purged entries as a linked list
    T* insert(T* entry) {
        size_t size = sizeOf(entry);
        T* purged = null;
        lock.lock();
        scope(exit) lock.unlock();
        if (size > availMemory) {
            purged = purgeBySize(size - availMemory);
        }
        purgeQueue = insertBack!"3"(purgeQueue, entry);
        availMemory -= size;
        return purged;
    }

    private T* purgeBySize(size_t size) {
        size_t purgedSize = 0;
        T* list = null;
        do {
            auto e = purgeQueue;
            purgeQueue = remove!"3"(e, e);
            e.next = list;
            list = e;
            purgedSize += sizeOf(e);
        } while(purgedSize < size);
        availMemory += purgedSize;
        return list;
    }

    // put the entry as the last in the purge queue
    void refresh(T* entry) {
        lock.lock();
        scope(exit) lock.unlock();
        if (entry.next3 != null) {
            purgeQueue = remove!"3"(purgeQueue, entry);
            purgeQueue = insertBack!"3"(purgeQueue, entry);
        }
    }

    // remove entry from purge list iff it's not already removed
    void purge(T* entry) {
        size_t size = sizeOf(entry);
        lock.lock();
        scope(exit) lock.unlock();
        if (entry.next3 != null) { // not already removed from LRU
            purgeQueue = remove!"3"(purgeQueue, entry);
            availMemory += size;
        }
    }
}

unittest {
    import std.algorithm, std.array;
    static struct Entry {
        size_t size;
        Entry* next;
        Entry* prev3, next3;
    }
    alias SimpleLru = Lru!(Entry, x => x.size);
    SimpleLru lru = SimpleLru(100);
    auto e50 = new Entry(50);
    auto e60 = new Entry(60);
    auto e20 = new Entry(20);
    assert(lru.insert(e50) == null);
    assert(lru.purgeQueue.toArray!"3".map!(x => x.size).array == [50]);
    auto purged = lru.insert(e60);
    assert(purged != null);
    assert(purged.size == 50);
    assert(lru.purgeQueue.toArray!"3".map!(x => x.size).array == [60]);
    assert(lru.insert(e20) == null);
    assert(lru.purgeQueue.toArray!"3".map!(x => x.size).array == [60, 20]);
    lru.refresh(e60);
    assert(lru.purgeQueue.toArray!"3".map!(x => x.size).array == [20, 60]);
    lru.purge(e60);
    assert(lru.purgeQueue.toArray!"3".map!(x => x.size).array == [20]);
    lru.refresh(e60);
    assert(lru.purgeQueue.toArray!"3".map!(x => x.size).array == [20]);
    lru.purge(e20);
    assert(lru.purgeQueue.toArray!"3".map!(x => x.size).array == []);
    assert(lru.availMemory == 100);
}