module memcached.cache;

import memcached.entry, memcached.lru, memcached.sched;


// set DELETING state if not already, true if we are the first to start deletion
bool startDeletion(Entry* entry) {
    return false;
}

