package top.lifeifan.db.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

public class RandomUtil {

    public static byte[] randomBytes(int length) {
        Random random = new SecureRandom();
        byte[] buf = new byte[length];
        return buf;
    }

}
