package top.lifeifan.db.backend.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author lifeifan
 * @since 2023-02-02
 */
public class Parser {

    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }


    public static byte[] long2Byte(Long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }


    public static short parseShort(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, 0, Short.SIZE / Byte.SIZE);
        return buf.getShort();
    }

    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int parseInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes, 0, Integer.SIZE / Byte.SIZE).getInt();
    }

    public static ParseStringRes parseString(byte[] bytes) {
        int length = parseInt(Arrays.copyOf(bytes, 4));
        String str = new String(Arrays.copyOfRange(bytes, 4, 4 + length));
        return new ParseStringRes(str, length + 4);
    }

    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for (byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }

    public static byte[] string2Byte(String str) {
        byte[] len = int2Byte(str.length());
        return Bytes.concat(len, str.getBytes());
    }
}
