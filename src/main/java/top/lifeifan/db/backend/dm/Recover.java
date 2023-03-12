package top.lifeifan.db.backend.dm;

import com.google.common.primitives.Bytes;
import top.lifeifan.db.backend.common.SubArray;
import top.lifeifan.db.backend.dm.dataItem.DataItem;
import top.lifeifan.db.backend.dm.logger.Logger;
import top.lifeifan.db.backend.dm.page.Page;
import top.lifeifan.db.backend.dm.page.PageX;
import top.lifeifan.db.backend.dm.pageCache.PageCache;
import top.lifeifan.db.backend.tm.TransactionManager;
import top.lifeifan.db.backend.utils.Panic;
import top.lifeifan.db.backend.utils.Parser;

import java.util.*;

public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long xid;
        int pgNo;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgNo;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();
        int maxPgNo = 0;
        while(true) {
            byte[] log = lg.next();
            if (log == null) {
                break;
            }
            int pgNo;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgNo = li.pgNo;
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                pgNo = xi.pgNo;
            }
            if (pgNo > maxPgNo) {
                maxPgNo = pgNo;
            }
        }
        if (maxPgNo == 0) {
            maxPgNo = 1;
        }
        pc.truncateByPageNo(maxPgNo);
        System.out.println("Truncate to " + maxPgNo + " pages.");

        redoTransactions(tm, lg, pc);
        System.out.println("Redo Transaction Over.");

        undoTransactions(tm, lg, pc);
        System.out.println("Undo Transaction Over.");

        System.out.println("Recovery Over.");
    }

    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) {
                break;
            }
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                // 如果插入事务已完成，就重新插入
                if (!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                // 如果更新事务已完成，就重新更新
                if (!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if (log == null) {
                break;
            }
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                InsertLogInfo xi = parseInsertLog(log);
                long xid = xi.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }


    // [LogType 1B] [XID 8B] [PgNo 4B] [Offset 2B] [Raw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    public static byte[] insertLog(long xid, Page page, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgNoRaw = Parser.int2Byte(page.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(page));
        return Bytes.concat(logTypeRaw, xidRaw, pgNoRaw, offsetRaw, raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgNo = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page page = null;
        try {
            page = pc.getPage(li.pgNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            if (flag == UNDO) {
              DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(page, li.raw, li.offset);
        }  finally {
            page.release();
        }
    }

    // [LogType 1B] [XID 8B] [UID 8B] [OldRaw] [NewRaw]
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logTypeRaw = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logTypeRaw, xidRaw, uidRaw, oldRaw, newRaw);
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgNo = (int) (uid & ((1L << 32) - 1));
        int oldOrNewRawLen = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_INSERT_RAW, OF_UPDATE_RAW + oldOrNewRawLen);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + oldOrNewRawLen, OF_UPDATE_RAW + 2 * oldOrNewRawLen);
        return li;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        byte[] raw;
        UpdateLogInfo xi = parseUpdateLog(log);
        int pgNo = xi.pgNo;
        short offset = xi.offset;
        if (flag == REDO) {
            raw = xi.newRaw;
        } else {
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }
}
