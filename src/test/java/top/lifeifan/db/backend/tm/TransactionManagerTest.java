package top.lifeifan.db.backend.tm;

import org.junit.After;
import org.junit.Test;

import java.io.File;

public class TransactionManagerTest {

    private static final String TEST_PATH = "D:\\lifei\\test_data\\20230202";

    private static final String XID_FILE_SUFFIX = ".xid";

    private TransactionManager tm;

    @Test
    public void testCreate() {
        tm = TransactionManager.create(TEST_PATH);
        assert tm.isValid();
        tm.close();
    }

    @Test
    public void testOpen() {
        tm = TransactionManager.open(TEST_PATH);
        assert tm.isValid();
    }

    @Test
    public void testBegin() {
        tm = TransactionManager.open(TEST_PATH);
        long xid = tm.begin();
        System.out.println(xid);
        assert tm.isActive(xid);
    }

    @Test
    public void testCommit() {
        tm = TransactionManager.open(TEST_PATH);
        long xid = 1L;
        tm.commit(xid);
        assert !tm.isAborted(xid) && !tm.isActive(xid);
        tm.close();
    }

    @Test
    public void testAbort() {
        tm = TransactionManager.open(TEST_PATH);
        long xid = 1L;
        tm.abort(xid);
        assert  tm.isAborted(xid);
        tm.close();
    }

    @After
    public void clear() {
        assert new File(TEST_PATH + XID_FILE_SUFFIX).delete();
    }
}
