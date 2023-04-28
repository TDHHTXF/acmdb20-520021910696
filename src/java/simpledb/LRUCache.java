package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LRUCache<K,V> {
    class pageNode {
        K key;
        V value;
        pageNode prev;
        pageNode next;
        public pageNode() {}
        public pageNode(K Key, V Value) {key = Key; value = Value;}

    }

    private Map<K, pageNode> cache = new ConcurrentHashMap<K, pageNode>();
    private int size;
    private int capacity;
    private pageNode head, tail;

    public LRUCache(int capacity) {
        this.size = 0;
        this.capacity = capacity;

        head = new pageNode();
        tail = new pageNode();
        head.next = tail;
        head.prev = tail;
        tail.prev = head;
        tail.next = head;
    }


    public int getSize() {
        return size;
    }

    public pageNode getHead() {
        return head;
    }

    public pageNode getTail() {
        return tail;
    }

    public Map<K, pageNode> getCache() {
        return cache;
    }

    public synchronized V get(K key) {
        pageNode node = cache.get(key);
        if (node == null) {
            return null;
        }

        moveToHead(node);
        return node.value;
    }
    public synchronized void remove(pageNode node){
        node.prev.next = node.next;
        node.next.prev = node.prev;
        cache.remove(node.key);
        size--;
    }

    public synchronized void discard(){

        pageNode tail = removeTail();

        cache.remove(tail.key);
        size--;

    }
    public synchronized void put(K key, V value) {
        pageNode node = cache.get(key);
        if (node == null) {

            pageNode newNode = new pageNode(key, value);

            cache.put(key, newNode);

            addToHead(newNode);
            ++size;
        }
        else {

            node.value = value;
            moveToHead(node);
        }
    }

    private void addToHead(pageNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(pageNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }


    private void moveToHead(pageNode node) {
        removeNode(node);
        addToHead(node);
    }

    private pageNode removeTail() {
        pageNode res = tail.prev;
        removeNode(res);
        return res;
    }

}
