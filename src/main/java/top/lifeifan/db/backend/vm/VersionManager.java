package top.lifeifan.db.backend.vm;

import top.lifeifan.db.backend.dm.DataManager;
import top.lifeifan.db.backend.tm.TransactionManager;

/**
 * vm层接口
 * @author lifeifan
 * @since 2023-02-26
 */
public interface VersionManager {

    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[]data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}
