package jlite.storage;
import java.util.*;
/**
 * LRU page cache.
 * TODO: dirty-page tracking, pin/unpin API, clock-sweep eviction.
 */
public class BufferPool {
    private final int capacity;
    private final Map<Integer, Page> cache;
    public BufferPool(int capacity) {
        this.capacity = capacity;
        this.cache = new LinkedHashMap<>(capacity, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry<Integer, Page> e) {
                return size() > BufferPool.this.capacity;
            }
        };
    }
    public synchronized Page fetchPage(int pageId) {
        return cache.computeIfAbsent(pageId, Page::new);
    }
}
