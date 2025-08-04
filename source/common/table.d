module common.table;


struct Table(K, V) {
private:
    static struct Node {
        K key;
        V value;
        size_t hash;
        Node* next;
    }
    Node*[] table;
    size_t items;
public:
    this(size_t size) {
        assert((size & (size-1)) == 0);
        table = new Node*[size];
    }

    size_t length() { return items; }

    V* lookup(K key, size_t hash) {
        auto ptr = &table[hash & (table.length-1)];
        while(*ptr) {
            if ((*ptr).key == key) {
                return &(*ptr).value;
            }
            ptr = &(*ptr).next;
        }
        return null;
    }

    V put(K key, size_t hash, V value) {
        if (items > table.length * 2) {
            rehash();
        }
        auto ptr = &table[hash & (table.length-1)];
        while (*ptr) {
            if ((*ptr).hash == hash && (*ptr).key == key) {
                auto old = (*ptr).value;
                (*ptr).key = key;
                (*ptr).value = value;
                return old;
            }
            ptr = &(*ptr).next;
        }
        *ptr = new Node(key, value, hash, null);
        items++;
        return V.init;
    }

    V remove(K key, size_t hash) {
        auto ptr = &table[hash & (table.length-1)];
        while (*ptr) {
            if ((*ptr).hash == hash && (*ptr).key == key) {
                auto value = (*ptr).value;
                *ptr = (*ptr).next;
                return value;
            }
            ptr = &(*ptr).next;
        }
        return V.init;
    }

    void rehash() {
        Table tab = Table(table.length * 2);
        foreach (node; table) {
            auto n = node;
            while (n) {
                tab.put(n.key, n.hash, n.value);
                n = n.next;
            }
        }
        this.table = tab.table;
    }
}

version(unittest) {
    V insert(K, V)(ref Table!(K,V) table, K key, V value) {
        return table.put(key, key.hashOf, value);
    }
    V* find(K, V)(ref Table!(K,V) table, K key) {
        return table.lookup(key, key.hashOf);
    }
}

unittest {
    auto t = Table!(string, int)(32);
    auto old = t.insert("hello", 11);
    assert(old == 0);
    assert(*t.find("hello") == 11);
    old = t.insert("hello", 42);
    assert(old == 11);
    assert(*t.find("hello") == 42);
    t.remove("hello", "hello".hashOf);
    assert(t.find("hello") == null);
}

unittest {
    auto t = Table!(int, int)(32);
    foreach (i; 0..128) {
        t.insert(i, i);
    }
    assert(t.table.length == 64);
    foreach (i; 0..128) {
        assert(*t.find(i) == i);
    }
}