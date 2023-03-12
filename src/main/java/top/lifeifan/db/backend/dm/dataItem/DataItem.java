package top.lifeifan.db.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import top.lifeifan.db.backend.common.SubArray;
import top.lifeifan.db.backend.dm.DataManagerImpl;
import top.lifeifan.db.backend.dm.page.Page;
import top.lifeifan.db.backend.utils.Parser;
import top.lifeifan.db.backend.utils.Types;

import java.util.Arrays;

/**
 * DM层向上的抽象
 * 上层模块通过地址向 DM 请求到相应的 DataItem，再获取到其中的数据
 *
 * @author lifeifan
 * @since 2023-02-05
 */
public interface DataItem {

    int OF_VALID = 0;
    int OF_SIZE = 1;
    int OF_DATA = 3;

    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    static void setDataItemRawInvalid(byte[] raw) {
        raw[OF_VALID] = (byte)1;
    }

    static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    static DataItem parseDataItem(Page page, short offset, DataManagerImpl dm) {
        byte[] raw = page.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + OF_SIZE, offset + OF_DATA));
        short length = (short) (size + OF_DATA);
        long uid = Types.addressToUid(page.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length),
                new byte[length], dm, uid, page);
    }
}
