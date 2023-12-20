module common.intrusive_queue;

enum isIntrusive(T) = __traits(compiles, () {
    T t;
    t.next = &t;
    t.prev = &t;
});

T* insert(T)(T* list, T* item) 
if (isIntrusive!T) {
    if (list == null) {
        item.next = item;
        item.prev = item;
    } else {
        item.prev = list.prev;
        item.next = list;
        list.prev = item;
    }
    return item;
}

T* remove(T)(T* list, T* item)
if (isIntrusive!T) {
    auto next = item.next;
    auto prev = item.prev;
    if (next == item) {
        item.next = null;
        item.prev = null;
        return null;
    } else {
        next.prev = prev;
        prev.next = next;
        item.next = null;
        item.prev = null;
        return next;
    }
}

unittest {
    static struct Node {
        int value;
        Node* prev, next;
    }
    Node* list;
    list = insert(list, new Node(1));
    assert(list.value == 1);
    list = insert(list, new Node(2));
    assert(list.value == 2);
    list = remove(list, list);
    assert(list.value == 1);
    list = remove(list, list);
    assert(list == null);
}