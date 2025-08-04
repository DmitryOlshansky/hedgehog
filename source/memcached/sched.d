module memcached.sched;

import common.intrusive_queue, memcached.entry;

import core.atomic, core.time, core.thread;

import std.datetime;

private:

enum RANGE = 3600;

enum Op {
    ADD,
    REMOVE,
    REFRESH
}

struct Ticket(T) {
    Op op;
    T* item;
    long expTime; // only used with refresh
    Ticket!T* next;
}

public long unixTime() {
    return Clock.currStdTime() / 10_000_000;
}

public struct Sched(T, alias time, alias onExpiration) {
    private shared Ticket!T* buffer;
    private shared bool isRunning;
    private shared long currentTick;
    private T*[RANGE] timerSlots;
    private Thread thread;

    private void scheduleOp(T* item, Op op, long expTime) {
        Ticket!T* old;
        Ticket!T* ticket = new Ticket!T(op, item, expTime, null);
        do {
            old = cast(Ticket!T*)atomicLoad(buffer);
            ticket.next = old;
        } while (!cas(&buffer, cast(shared)old, cast(shared)ticket));
    }

    void schedule(T* item) {
        return scheduleOp(item, Op.ADD, 0);
    }

    void refresh(T* item, long expTime) {
        return scheduleOp(item, Op.REFRESH, expTime);
    }

    void deschedule(T* item) {
        return scheduleOp(item, Op.REMOVE, 0);
    }

    void start() {
        assert(isRunning == false);
        atomicStore(currentTick, time());
        atomicStore(isRunning, true);
        thread = new Thread(() {
            import std.stdio;
            while(atomicLoad(isRunning)) {
                Thread.sleep(dur!"msecs"(1));
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
            list = remove!"2"(list, item);
            if (item.expTime == current)
                onExpiration(item);
            else
                newList = insertFront!"2"(newList, item);
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
                    if(reversed.item.next2 == null) {
                        timerSlots[slot] = insertFront!"2"(timerSlots[slot], reversed.item);
                    }
                    break;
                case REMOVE:
                    if(reversed.item.next2 != null) {
                        timerSlots[slot] = remove!"2"(timerSlots[slot], reversed.item);
                        reversed.item.release();
                    }
                    break;
                case REFRESH:
                    if (reversed.item.next2 != null) {
                        timerSlots[slot] = remove!"2"(timerSlots[slot], reversed.item);
                        reversed.item.expTime = reversed.expTime;
                        auto slot2 = reversed.expTime % RANGE;
                        timerSlots[slot2] = insertFront!"2"(timerSlots[slot2], reversed.item);
                    }
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
        long expTime;
        Entry* next;
        Entry* prev2, next2;
    }
    
    Entry*[] expired;
    int lt = 0;
    
    int logicalTime() {
        return lt;
    }
    
    void onExpire(Entry* e) {
        expired ~= e;
    }

    Sched!(Entry, logicalTime, onExpire) sched;
    sched.schedule(new Entry("A1", 1));
    sched.schedule(new Entry("A2", 1));
    sched.schedule(new Entry("B1", 2));
    sched.schedule(new Entry("B2", 2));
    sched.processQueue();
    sched.tick(); // process 0 time
    assert(expired.length == 0);
    sched.tick(); // process 1 time
    assert(expired.length == 2);
    assert(expired.map!(x => x.name).array == ["A2", "A1"]);
    sched.tick();
    assert(expired.length == 4);
    assert(expired.map!(x => x.name).array == ["A2", "A1", "B2", "B1"]);
    sched.schedule(new Entry("C1", 2)); // schedule with already expired time
    sched.processQueue();
    assert(expired.map!(x => x.name).array == ["A2", "A1", "B2", "B1", "C1"]);
}