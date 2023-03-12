package top.lifeifan.db.backend.vm;

import top.lifeifan.db.backend.utils.Panic;
import top.lifeifan.db.common.Error;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护一个依赖等待图，以进行死锁检测
 * @since 2023-02-25
 * @author lifeifan
 */
public class LockTable {

    // 某个XID已经获得的资源的UID列表
    private Map<Long, List<Long>> x2u;
    // UID被某个XID持有
    private Map<Long, Long> u2x;
    // 正在等待UID的XID列表
    private Map<Long, List<Long>> wait;
    // 正在等待资源的XID的锁
    private Map<Long, Lock> waitLock;
    // XID正在等待的UID
    private Map<Long, Long> waitU;

    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 尝试分配资源uid给xid，如果需要等待则返回锁对象，如果会造成死锁，则抛出异常
     * @param xid 事务id
     * @param uid 资源id
     * @return lock or null
     * @throws Exception e
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // 如果xid已经持有uid
            if (isInList(x2u, xid, uid)) {
                return null;
            }
            // 如果uid没有被其他事务持有
            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            // xid等待uid
            waitU.put(xid, uid);
            // 将xid放入正在等待uid的事务id列表
            putIntoList(wait, xid, uid);
            if (hasDeadLock()) {
                // 如果死锁
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                Panic.throwException(Error.DeadLockException);
            }
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 在一个事务commit或abort时，就释放它所有的锁，并从等待图中删除
     * @param xid
     */
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> uidList = x2u.get(xid);
            if (uidList != null) {
                while (uidList.size() > 0) {
                    Long uid = uidList.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从等待队列中选择一个xid来占用uid
     * @param uid
     */
    private void selectNewXID(Long uid) {
        u2x.remove(uid);
        List<Long> xidList = wait.get(uid);
        if (xidList == null)
            return;
        assert xidList.size() > 0;
        while (xidList.size() > 0) {
            // 按照先来先到顺序尝试解锁——公平锁
            long xid = xidList.remove(0);
            if (xidList.size() == 0)
                wait.remove(uid);
            if (!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                Lock xlock = waitLock.remove(xid);
                waitU.remove(xid);
                xlock.unlock();
                break;
            }
        }
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for (long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if (s != null && s > 0) {
                continue;
            }
            stamp++;
            if (dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if (stp != null && stp == stamp) {
            return true;
        }
        if (stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        if (uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, Long key, Long value) {
        if (!listMap.containsKey(key))
            return false;
        return listMap.get(key).contains(value);
    }

    private void putIntoList(Map<Long, List<Long>> listMap, Long key, Long value) {
        if (!listMap.containsKey(key))
            listMap.put(key, new ArrayList<>());
        listMap.get(key).add(value);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, Long key, Long value) {
        if (!listMap.containsKey(key))
            return;
        listMap.get(key).remove(value);
    }
}
