package top.lifeifan.db.backend.dm.page;

import top.lifeifan.db.backend.dm.pageCache.PageCache;
import top.lifeifan.db.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时在 100~107处填充一个随机字节，db关闭时将其拷贝到 108~115字节
 * 以此来判断上一次数据库是否正常关闭
 * @author lifeifan
 * @since 2023-02-04
 */
public class PageOne {

    private static final Integer LEN_VC = 8;
    private static final Integer OF_VC = 100;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page page) {
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page page) {
        page.setDirty(true);
        setVcClose(page.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page page) {
        return checkVc(page.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC),
                Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC));
    }



}
