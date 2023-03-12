package top.lifeifan.db.backend.dm;

import top.lifeifan.db.backend.common.AbstractCache;
import top.lifeifan.db.backend.dm.dataItem.DataItem;
import top.lifeifan.db.backend.dm.dataItem.DataItemImpl;
import top.lifeifan.db.backend.dm.logger.Logger;
import top.lifeifan.db.backend.dm.page.Page;
import top.lifeifan.db.backend.dm.page.PageOne;
import top.lifeifan.db.backend.dm.page.PageX;
import top.lifeifan.db.backend.dm.pageCache.PageCache;
import top.lifeifan.db.backend.dm.pageIndex.PageIndex;
import top.lifeifan.db.backend.dm.pageIndex.PageInfo;
import top.lifeifan.db.backend.tm.TransactionManager;
import top.lifeifan.db.backend.utils.Panic;
import top.lifeifan.db.backend.utils.Types;
import top.lifeifan.db.common.Error;

/**
 * @author lifeifan
 * @since 2023-02-06
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{

    TransactionManager tm;
    PageCache pageCache;
    Logger logger;
    PageIndex pageIndex;
    Page pageOne;

    public DataManagerImpl(TransactionManager tm, PageCache pageCache, Logger logger) {
        super(0);
        this.tm = tm;
        this.pageCache = pageCache;
        this.logger = logger;
        this.pageIndex = new PageIndex();
    }

    void fillPageIndex() {
        // 得到页数
        int pageNumber = pageCache.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) {
            Page page = null;
            try {
                page = pageCache.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pageIndex.add(page.getPageNumber(), PageX.getFreeSpace(page));
            page.release();
        }
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItem = (DataItemImpl) super.get(uid);
        if (!dataItem.isValid()) {
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            Panic.panic(Error.DataToolLargeException);
        }
        // 尝试获取可用页
        PageInfo pageInfo = null;
        for (int i = 0; i < 5; i++) {
            pageInfo = pageIndex.select(raw.length);
            if (pageInfo != null) {
                break;
            } else {
                int newPgNo = pageCache.newPage(PageX.initRaw());
                pageIndex.add(newPgNo, PageX.MAX_FREE_SPACE);
            }
        }
        if (pageInfo == null) {
            Panic.panic(Error.DatabaseBusyException);
        }
        Page page = null;
        int freeSpace = 0;
        try {
            page = pageCache.getPage(pageInfo.getPgNo());
            byte[] log = Recover.insertLog(xid, page, raw);
            logger.log(log);

            short offset = PageX.insert(page, raw);

            page.release();
            return Types.addressToUid(pageInfo.getPgNo(), offset);
        } finally {
            if (page != null) {
                pageIndex.add(pageInfo.getPgNo(), PageX.getFreeSpace(page));
            } else {
                pageIndex.add(pageInfo.getPgNo(), freeSpace);
            }
        }
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgNo = (int)(uid & ((1L << 32) - 1));
        Page pg = pageCache.getPage(pgNo);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pageCache.close();
    }

    public void initPageOne() {
        int pgNo = pageCache.newPage(PageOne.initRaw());
        assert pgNo == 1;
        try {
            pageOne = pageCache.getPage(pgNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pageCache.flushPage(pageOne);
    }

    /**
     * 在打开已有文件时读入 PageOne，并验证正确性
     * @return
     */
    public boolean loadCheckPageOne() {
        try {
            pageOne = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 为 xid 生成 update 日志
     * @param xid
     * @param dataItem
     */
    public void logDataItem(long xid, DataItemImpl dataItem) {
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);
    }

    public void releaseDataItem(DataItemImpl dataItem) {
        super.release(dataItem.getUid());
    }
}
