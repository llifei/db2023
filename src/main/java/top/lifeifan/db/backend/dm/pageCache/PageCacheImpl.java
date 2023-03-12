package top.lifeifan.db.backend.dm.pageCache;

import top.lifeifan.db.backend.common.AbstractCache;
import top.lifeifan.db.backend.dm.page.Page;
import top.lifeifan.db.backend.dm.page.PageImpl;
import top.lifeifan.db.backend.utils.Panic;
import top.lifeifan.db.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lifeifan
 * @since 2023-02-04
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final Integer MEM_MIN_LIM = 10;
    public static final String DB_FILE_SUFFIX = ".db";

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private Lock fileLock;

    // 当前打开的数据库文件的页数
    private AtomicInteger pageNumbers;

    public PageCacheImpl(int maxResource, RandomAccessFile randomAccessFile,
                         FileChannel fileChannel) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = randomAccessFile.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.randomAccessFile = randomAccessFile;
        this.fileChannel = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) (length / PAGE_SIZE));
    }

    /**
     * 根据pageNo从数据库文件中读取页数据，并构造成Page
     * @param key pageNo
     * @return page
     * @throws Exception e
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pageNo = (int) key;
        long offset = PageCacheImpl.pageOffset(pageNo);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pageNo, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page page) {
        if (page.isDirty()) {
            flush(page);
            page.setDirty(false);
        }
    }

    @Override
    public int newPage(byte[] initData) {
        int pageNo = pageNumbers.incrementAndGet();
        Page page = new PageImpl(pageNo, initData, null);
        // 新建的页面需要立刻写回
        flushPage(page);
        return pageNo;
    }

    @Override
    public Page getPage(int pageNo) throws Exception {
        return get(pageNo);
    }

    @Override
    public void close() {
        super.close();
        try {
            fileChannel.close();
            randomAccessFile.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    /**
     * 截断数据库文件到指定页码
     * @param maxPageNo 最大页码
     */
    @Override
    public void truncateByPageNo(int maxPageNo) {
        long size = pageOffset(maxPageNo + 1);
        try {
            randomAccessFile.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNo);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }

    /**
     * 将page内数据刷入磁盘文件
     * @param page page
     */
    private void flush(Page page) {
        int pageNo = page.getPageNumber();
        long offset = pageOffset(pageNo);
        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fileChannel.position(offset);
            fileChannel.write(buf);
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * 计算指定页在文件中的位置，页码从1开始算
     * @param pageNo
     * @return
     */
    private static long pageOffset(int pageNo) {
        return (long) (pageNo - 1) * PAGE_SIZE;
    }
}
