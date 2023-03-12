package top.lifeifan.db.backend.utils;

import lombok.extern.slf4j.Slf4j;
import top.lifeifan.db.common.Error;
import top.lifeifan.db.common.OperationFailException;

/**
 * @author lifeifan
 * @since 2023-02-02
 */
@Slf4j
public class Panic {

    public static void panic(Error e) {
        log.error(e.getMessage());
        System.exit(1);
    }

    public static void panic(Exception e) {
        e.printStackTrace();
        System.exit(1);
    }

    public static void throwException(Error e) throws Exception {
        throw new OperationFailException(e.getMessage());
    }

}
