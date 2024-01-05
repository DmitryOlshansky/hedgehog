module memcached.timer_queue;

import common.intrusive_queue;

import core.atomic, core.time, core.thread;

import std.datetime;

private:

enum RANGE = 3600;

enum Op {
    ADD,
    REMOVE
}

struct Ticket(T) {
    Op op;
    T* item;
    Ticket!T* next;
}

public long unixTime() {
    return Clock.currStdTime() / 10_000_000;
}


public struct TimerQueue(T, alias time, alias onExpiration) {
    private shared Ticket!T* buffer;
    private shared bool isRunning;
    private shared long currentTick;
    private T*[RANGE] timerSlots;
    private Thread thread;

    private void scheduleOp(T* item, Op op) {
        Ticket!T* old;
        Ticket!T* ticket = new Ticket!T(op, item, null);
        do {
            old = cast(Ticket!T*)atomicLoad(buffer);
            ticket.next = old;
        } while (!cas(&buffer, cast(shared)old, cast(shared)ticket));
    }

    void schedule(T* item) {
        return scheduleOp(item, Op.ADD);
    }

    void deschedule(T* item) {
        return scheduleOp(item, Op.REMOVE);
    }

    void start() {
        assert(isRunning == false);
        atomicStore(currentTick, time());
        atomicStore(isRunning, true);
        thread = new Thread(() {
            import std.stdio;
            while(atomicLoad(isRunning)) {
                Thread.sleep(dur!"msecs"(25));
                long seconds = time();
                // process all enqueued work, move to/from timerSlots
                processQueue();
                // process ticks
                while(atomicLoad(currentTick) < seconds) {
                    tick();
                }
            }
        });
        thread.isDaemon = true;
        thread.start();
    }

    void stop() {
        isRunning = false;
    }

    long currentTime() {
        return atomicLoad(currentTick);
    }

    private void tick() {
        long current = atomicFetchAdd(currentTick, 1);
        long slot = current % RANGE;
        T* list = timerSlots[slot];
        T* newList = null;
        timerSlots[slot] = null;
        while (list != null) {
            T* item = list;
            list = remove!""(list, item);
            if (item.expTime == current)
                onExpiration(item);
            else
                newList = insertFront!""(newList, item);
        }
        timerSlots[slot] = newList;
    }

    private Ticket!T* getQueueed() {
        shared Ticket!T* current;
        do {
            current = buffer;
        } while (!cas(&buffer, current, null));
        Ticket!T* head = cast(Ticket!T*)current;
        Ticket!T* reversed = null;
        while (head != null) {
            auto next = head.next;
            head.next = reversed;
            reversed = head;
            head = next;
        }
        return reversed;
    }

    private void processQueue() {
        auto reversed = getQueueed();
        auto tick = atomicLoad(currentTick);
        while (reversed != null) {
            if (reversed.item.expTime > tick) {
                auto slot = reversed.item.expTime % RANGE;
                final switch(reversed.op) with (Op) {
                case ADD:
                    assert(reversed.item.next == null);
                    assert(reversed.item.prev == null);
                    timerSlots[slot] = insertFront!""(timerSlots[slot], reversed.item);
                    break;
                case REMOVE:
                    assert(reversed.item.next != null);
                    timerSlots[slot] = remove!""(timerSlots[slot], reversed.item);
                    break;
                }
            }
            else {
                onExpiration(reversed.item);
            }
            reversed = reversed.next;
        }
    }
}

unittest {
    import std.array, std.algorithm;
    static struct Entry {
        string name;
        int expTime;
        Entry* prev, next;
    }
    
    Entry*[] expired;
    int lt = 0;
    
    int logicalTime() {
        return lt;
    }
    
    void onExpire(Entry* e) {
        expired ~= e;
    }

    TimerQueue!(Entry, logicalTime, onExpire) timerQueue;
    timerQueue.schedule(new Entry("A1", 1));
    timerQueue.schedule(new Entry("A2", 1));
    timerQueue.schedule(new Entry("B1", 2));
    timerQueue.schedule(new Entry("B2", 2));
    timerQueue.processQueue();
    timerQueue.tick(); // process 0 time
    assert(expired.length == 0);
    timerQueue.tick(); // process 1 time
    assert(expired.length == 2);
    assert(expired.map!(x => x.name).array == ["A2", "A1"]);
    timerQueue.tick();
    assert(expired.length == 4);
    assert(expired.map!(x => x.name).array == ["A2", "A1", "B2", "B1"]);
    timerQueue.schedule(new Entry("C1", 2)); // schedule with already expired time
    timerQueue.processQueue();
    assert(expired.map!(x => x.name).array == ["A2", "A1", "B2", "B1", "C1"]);
}