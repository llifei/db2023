package top.lifeifan.db.backend.dm.logger;

import org.junit.Test;

import java.io.File;

public class LoggerTest {
    private static final String LOG_FILE = "D:\\lifei\\test_data\\logger_test";
    private static final String LOG_SUFFIX = ".log";

    @Test
    public void testLogger() {
        Logger lg = Logger.create(LOG_FILE);
        lg.log("aaa".getBytes());
        lg.log("bbb".getBytes());
        lg.log("ccc".getBytes());
        lg.log("ddd".getBytes());
        lg.log("eee".getBytes());
        lg.close();

        lg = Logger.open(LOG_FILE);
        lg.rewind();

        byte[] log = lg.next();
        assert log != null;
        assert "aaa".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "bbb".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "ccc".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "ddd".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "eee".equals(new String(log));

        log = lg.next();
        assert log == null;

        lg.close();

        assert new File(LOG_FILE + LOG_SUFFIX).delete();
    }

}
