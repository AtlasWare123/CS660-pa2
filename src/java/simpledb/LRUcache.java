package simpledb;

import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.NoSuchElementException;
import java.util.Set;


public class LRUcache {
    int cap;
    LinkedHashMap<PageId, Page> cache;
    public LRUcache(int capacity) {
        this.cap = capacity;
        cache = new LinkedHashMap<>();
    }

    public Page get(PageId key) throws Exception {
        if (!cache.containsKey(key)) {
            throw new Exception("no such key in cache");
        }
        // 将 key 变为最近使用
        makeRecently(key);
        return cache.get(key);
    }

    public void put(PageId key, Page val) {
        if (cache.containsKey(key)) {
            // 修改 key 的值
            cache.put(key, val);
            // 将 key 变为最近使用
            makeRecently(key);
            return;
        }

        if (cache.size() >= this.cap) {
            // 链表头部就是最久未使用的 key
            PageId oldestKey = cache.keySet().iterator().next();
            cache.remove(oldestKey);
        }
        // 将新的 key 添加链表尾部
        cache.put(key, val);
    }

    private void makeRecently(PageId key) {
        Page val = cache.get(key);
        // 删除 key，重新插入到队尾
        cache.remove(key);
        cache.put(key, val);
    }
}
