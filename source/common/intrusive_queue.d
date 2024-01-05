module common.intrusive_queue;

enum isIntrusive(T, string suffix) = __traits(compiles, () {
    T t;
    mixin("t.next"~suffix) = &t;
    mixin("t.prev"~suffix) = &t;
});

T* insertFront(string suffix, T)(T* list, T* item) 
if (isIntrusive!(T, suffix)) {
    if (list == null) {
        mixin("item.next"~suffix) = item;
        mixin("item.prev"~suffix) = item;
    }
    else {
        mixin("item.prev"~suffix) = mixin("list.prev"~suffix);
        mixin("item.next"~suffix) = list;
        mixin("list.prev"~suffix~".next"~suffix) = item;
        mixin("list.prev"~suffix) = item;
    }
    return item;
}

T* insertBack(string suffix, T)(T* list, T* item)
if (isIntrusive!(T, suffix)) {
    if (list == null) {
        mixin("item.next"~suffix) = item;
        mixin("item.prev"~suffix) = item;
        return item;
    }
    else {
        mixin("item.prev"~suffix) = mixin("list.prev"~suffix);
        mixin("item.next"~suffix) = list;
        mixin("list.prev"~suffix~".next"~suffix) = item;
        mixin("list.prev"~suffix) = item;
        return list;
    }
}


T* remove(string suffix, T)(T* list, T* item)
if (isIntrusive!(T, suffix)) {
    auto next = mixin("item.next"~suffix);
    auto prev = mixin("item.prev"~suffix);
    mixin("item.next"~suffix) = null;
    mixin("item.prev"~suffix) = null;
    if (next == item) {
        return null;
    }
    else {
        mixin("next.prev"~suffix) = prev;
        mixin("prev.next"~suffix) = next;
        if (item == list) {
            return next;
        }
        else {
            return list;
        }
    }
}

version(unittest) {
    Node*[] toArray(string suffix, Node)(Node* list) {
        Node*[] items;
        Node* head = list;
        if (head == null)
            return items;
        Node* cur = head;
        do {
            items ~= cur;
            cur = mixin("cur.next"~suffix);
        } while(cur != head);
        return items;
    }
}

unittest {
    static struct Node {
        int value;
        Node* prev, next;
    }
    Node* list;
    list = insertFront!""(list, new Node(1));
    assert(list.value == 1);
    list = insertFront!""(list, new Node(2));
    assert(list.value == 2);
    list = remove!""(list, list);
    assert(list.value == 1);
    list = remove!""(list, list);
    assert(list == null);
}

unittest {
    import std.algorithm, std.array, std.meta;
    foreach (suffix; AliasSeq!("", "2")) {
        static struct Node {
            int value;
            mixin("Node* prev"~suffix~", next"~suffix~";");
        }
        Node* list;
        list = insertBack!suffix(list, new Node(1));
        assert(list.value == 1);
        list = insertBack!suffix(list, new Node(2));
        assert(list.value == 1);
        list = insertBack!suffix(list, new Node(3));
        assert(list.value == 1);
        list = remove!suffix(list, list);
        assert(list.value == 2);
        list = remove!suffix(list, list);
        assert(list.value == 3);
        list = remove!suffix(list, list);
        assert(list == null);

        Node* f = new Node(1), m = new Node(2), t = new Node(3);
        list = insertBack!suffix(list, f);
        list = insertBack!suffix(list, m);
        list = insertBack!suffix(list, t);
        remove!suffix(list, m);
        auto items = list.toArray!suffix;
        assert(items.map!(x => x.value).array == [1, 3]);
    }
}