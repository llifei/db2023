package top.lifeifan.db.backend.dm.pageCache;

import javafx.util.Pair;
import top.lifeifan.db.backend.dm.page.Page;
import top.lifeifan.db.backend.utils.FileUtil;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author lifeifan
 * @since 2023-02-04
 */
public interface PageCache {

    // 页空间大小为 8KB
    Integer PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);
    Page getPage(int pageNo) throws Exception;
    void close();
    void release(Page page);

    void truncateByPageNo(int maxPageNo);
    int getPageNumber();
    void flushPage(Page page);

    static PageCacheImpl create(String path, long memory) {
        File f= new File(path + PageCacheImpl.DB_FILE_SUFFIX);
        FileUtil.createFileCanRW(f);
        Pair<RandomAccessFile, FileChannel> pair = FileUtil.getRafAndChannel(f);
        return new PageCacheImpl((int) (memory / PAGE_SIZE),pair.getKey(), pair.getValue());
    }

    static PageCacheImpl open(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_FILE_SUFFIX);
        FileUtil.checkRW(f);
        Pair<RandomAccessFile, FileChannel> pair = FileUtil.getRafAndChannel(f);
        return new PageCacheImpl((int) (memory / PAGE_SIZE),pair.getKey(), pair.getValue());
    }
}
