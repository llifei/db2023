package top.lifeifan.db.backend.dm.pageIndex;

import top.lifeifan.db.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面索引
 *
 * 缓存每一页的空闲空间，用于在上层模块进行插入操作时，
 * 能够快速找到一个合适空间的页面，而无需从磁盘或缓存中检查每一个页面的信息
 *
 * @author lifeifan
 * @since 2023-02-06
 */
public class PageIndex {

    // 一页划成 40 个区间
    private static final int INTERVALS_NO = 40;
    // 每个区间的大小为 8KB / 40
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] pageInfos;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        pageInfos = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            pageInfos[i] = new ArrayList<>();
        }
    }

    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 根据空闲空间大小得到可以容纳的最小索引号
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO) {
                number++;
            }
            while (number <= INTERVALS_NO) {
                if (pageInfos[number].size() == 0) {
                    number++;
                    continue;
                }
                // 同一个页面不允许并发写，在上层模块使用完后需要重新插入 PageIndex
                return pageInfos[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    public void add(int pgNo, int freeSpace) {
        lock.lock();
        try {
            // 根据空闲空间大小得到索引号
            int number = freeSpace / THRESHOLD;
            pageInfos[number].add(new PageInfo(pgNo, freeSpace));
        } finally {
            lock.unlock();
        }
    }

}
