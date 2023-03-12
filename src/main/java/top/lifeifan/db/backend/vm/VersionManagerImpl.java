package top.lifeifan.db.backend.vm;

import lombok.extern.slf4j.Slf4j;
import top.lifeifan.db.backend.common.AbstractCache;
import top.lifeifan.db.backend.dm.DataManager;
import top.lifeifan.db.backend.tm.TransactionManager;
import top.lifeifan.db.backend.tm.TransactionManagerImpl;
import top.lifeifan.db.backend.utils.Panic;
import top.lifeifan.db.common.OperationFailException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lockTable;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID,
                Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lockTable = new LockTable();
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if (entry == null) {
            throw new OperationFailException("Null Entry!");
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    /**
     * 读取一个 entry
     * @param xid 事务id
     * @param uid uid
     * @return entry内容
     * @throws Exception e
     */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if (t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e.getMessage().equals("Null Entry!")) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if (Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if (t.err != null) {
            throw t.err;
        }
        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if (t.err != null) {
            throw t.err;
        }
        Entry entry = super.get(uid);
        try {
            // 可见性判断
            if (!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            // 获取资源的锁
            Lock ulock = null;
            try {
                // add得到的lock是锁定状态
                ulock = lockTable.add(xid, uid);
            } catch (Exception e) {
                // 事务t异常
                t.err = OperationFailException.ConcurrentUpdateException;
                // 自动撤销事务
                autoAbortTransaction(t, xid);
            }
            // 等待uid的锁使用lock来阻塞，等待成功获取uid后立即unlock
            if (ulock != null) {
                ulock.lock();
                ulock.unlock();
            }
            // 已被删除
            if (entry.getXmax() == xid) {
                return false;
            }
            // 版本跳跃判断
            if (Visibility.isVersionSkip(tm, t, entry))  {
                t.err = OperationFailException.ConcurrentUpdateException;
                autoAbortTransaction(t, xid);
            }
            // 记录删除操作
            entry.setXmax(xid);
            return true;
        } finally {
            entry.release();
        }
    }

    private void autoAbortTransaction(Transaction t, long xid) throws Exception {
        internAbort(xid, true);
        t.autoAborted = true;
        throw t.err;
    }

    /**
     * 开启一个新事务
     * @param level
     * @return
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            // 将其放在activeTransaction中，用于检查和快照使用
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 提交一个事务
     * @param xid
     */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        try {
            if (t.err != null) {
                throw t.err;
            }
        } catch (NullPointerException n) {
            log.error(xid + " transaction is null, may be not active");
            log.error("now active transactions are: " + activeTransaction.keySet());
            Panic.panic(n);
        }
        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();
        // 释放所有与xid有关的锁，从等待图中删除
        lockTable.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    /**
     * 两种方式撤销事务
     * @param xid 事务id
     * @param autoAborted 是否自动
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if (!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();
        if (t.autoAborted) {
            return;
        }
        lockTable.remove(xid);
        tm.abort(xid);
    }
}
