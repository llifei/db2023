package top.lifeifan.db.backend.vm;

import com.google.common.primitives.Bytes;
import top.lifeifan.db.backend.common.SubArray;
import top.lifeifan.db.backend.dm.dataItem.DataItem;
import top.lifeifan.db.backend.utils.Parser;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 一条记录就是一条Entry，存储在DataItem中
 * 一条 Entry 中存储的数据格式为：[XMIN][XMAX][DATA]
 *          XMIN：创建该条记录的事务编号
 *          XMAX：删除该条记录的事务编号
 *          DATA：该条记录持有的数据
 */
public class Entry {

    private static final Integer OF_XMIN = 0;
    private static final Integer OF_XMAX = OF_XMIN + 8;
    private static final Integer OF_DATA = OF_XMAX + 8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    private static Entry newEntry(VersionManager vm, DataItem di, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = di;
        entry.vm = vm;
        return entry;
    }

    public void remove() {
        dataItem.release();
    }

    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    /**
     * 创建记录
     * @param xid xmin
     * @param data data
     * @return entryRaw
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start + OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();;
        }
    }

    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start + OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMIN, sa.start + OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMAX, sa.start + OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getUid() {
        return this.uid;
    }
}
