package redis;

import java.util.LinkedHashMap;

/**
 * 使用LinkedHashMap实现LRU算法而不是软引用，弱引用实现缓存。
 *
 * @author tanjionghong
 * @version 1.0
 * @since 2018-02-18
 **/
public class LRULocalCache<K, V> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = 1L;

    private int cacheSize;

    public LRULocalCache(int initialCapacity,
                         float loadFactor,
                         boolean accessOrder,
                         int cacheSize) {
        super(initialCapacity, loadFactor, accessOrder);
        this.cacheSize = cacheSize;
    }

    /**
     * @param eldest
     * @description 重写LinkedHashMap中的removeEldestEntry方法，当LRU中元素多于cachesize个时，
     * 删除最不经常使用的元素
     */
    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
        return size() > cacheSize;
    }

    public static void main(String[] args) {

        LRULocalCache<Character, Integer> lru = new LRULocalCache<Character, Integer>(
                16, 0.75f, true, 6);

        String s = "abcdefghijkl";
        for (int i = 0; i < s.length(); i++) {
            lru.put(s.charAt(i), i);
        }
        System.out.println("LRU中key为h的Entry的值为： " + lru.get('h'));
        System.out.println("LRU的大小 ：" + lru.size());
        System.out.println("LRU ：" + lru);
    }
}
