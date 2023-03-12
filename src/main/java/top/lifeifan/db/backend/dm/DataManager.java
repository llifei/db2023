package top.lifeifan.db.backend.dm;

import top.lifeifan.db.backend.dm.dataItem.DataItem;
import top.lifeifan.db.backend.dm.logger.Logger;
import top.lifeifan.db.backend.dm.page.PageOne;
import top.lifeifan.db.backend.dm.pageCache.PageCache;
import top.lifeifan.db.backend.tm.TransactionManager;

/**
 * DataManager
 *
 * @author lifeifan
 * @since 2023-02-06
 */
public interface DataManager {

    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(tm, pc, lg);
        dm.initPageOne();
        return dm;
    }

    static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);

        DataManagerImpl dm = new DataManagerImpl(tm, pc, lg);
        if (!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pageCache.flushPage(dm.pageOne);

        return dm;
    }

}
