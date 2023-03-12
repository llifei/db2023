package top.lifeifan.db.backend.vm;

import top.lifeifan.db.backend.tm.TransactionManager;

public class Visibility {

    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid= t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        // 由 t 创建，且还未删除
        if (xmin == xid && xmax == 0) {
            return true;
        }
        // 由一个已提交的事务创建
        if (tm.isCommitted(xmin)) {
            // 还未删除
            if (xmax == 0) {
                return true;
            }
            // 由一个未提交的事务删除
            if (xmax != xid && !tm.isCommitted(xmax)) {
                return true;
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //  由 t 创建且还未删除
        if (xmin == xid && xmax == 0)
            return true;
        // t事务开始之前的其他事务(t')创建，且t事务开始时t'不处于活跃状态
        if (xmin < xid && tm.isCommitted(xmin) && !t.isInSnapshot(xmin)) {
            // 此记录未被删除
            if (xmax == 0)
                return true;
            // 由其他事务(t'')删除
            if (xmax != xid) {
                // t''还未提交，或 t'' 在 t 开始之后才开始，或 t 开始时 t'' 处于活跃状态
                return !tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax);
            }
        }
        return false;
    }

    /**
     * 版本跳跃检查：取出要修改记录的最新提交版本，检查该最新版本的创建者对当前事务是否可见
     * @param tm transactionManager
     * @param t transaction
     * @param e entry
     * @return isVersionSkip
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if (t.level == 0) {
            // 已提交读级别不会出现版本跳跃
            return false;
        }
        // Tj对Ti不可见：XID(Tj)>XID(Ti) || Tj in SP(Ti)
        return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry entry) {
        if (t.level == 0) {
            return readCommitted(tm, t, entry);
        } else {
            return repeatableRead(tm, t, entry);
        }
    }
}
