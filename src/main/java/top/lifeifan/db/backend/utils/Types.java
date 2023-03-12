package top.lifeifan.db.backend.utils;

public class Types {
    public static long addressToUid(int pgNo, short offset) {
        long u0 = pgNo;
        long u1 = offset;
        return u0 << 32 | u1;
    }
}
