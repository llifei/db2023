package top.lifeifan.db.backend.common;

import top.lifeifan.db.backend.utils.Panic;
import top.lifeifan.db.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 引用计数策略的缓存实现
 * @author lifeifan
 * @since 2023-02-03
 * @param <T> 数据
 */
public abstract class AbstractCache <T>{

    protected abstract T getForCache(long key) throws Exception;

    protected abstract void releaseForCache(T obj);

    // 实际缓存的数据
    private Map<Long, T> cache;
    // 资源的引用个数
    private Map<Long, Integer> references;
    // 正在被获取的资源
    private Map<Long, Boolean> getting;

    // 缓存中的最大缓存资源数
    private int maxResource;
    // 缓存中元素的个数
    private int count = 0;

    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    public T get(long key) throws Exception {
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            if (cache.containsKey(key)) {
                // 资源在缓存中，且没有被其他线程正在获取
                T value = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return value;
            }

            // 尝试获取资源
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                Panic.panic(Error.CacheFullException);
            }
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T value = null;
        try {
            value = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        lock.lock();
        getting.remove(key);
        cache.put(key, value);
        references.put(key, 1);
        lock.unlock();

        return value;
    }

    /**
     * 释放一个缓存
     * @param key key
     */
    public void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T value = cache.get(key);
                releaseForCache(value);
                references.remove(key);
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
         } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T value = cache.get(key);
                releaseForCache(value);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }
}
