package top.lifeifan.db.backend.common;

/**
 * @author lifeifan
 * @since 2023-02-03
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
