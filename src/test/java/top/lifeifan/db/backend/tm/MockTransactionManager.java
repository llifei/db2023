package top.lifeifan.db.backend.tm;

public class MockTransactionManager implements TransactionManager {

    @Override
    public Long begin() {
        return 0L;
    }

    @Override
    public void commit(Long xid) {

    }

    @Override
    public void abort(Long xid) {

    }

    @Override
    public Boolean isActive(Long xid) {
        return false;
    }

    @Override
    public Boolean isAborted(Long xid) {
        return false;
    }

    @Override
    public Boolean isValid() {
        return false;
    }

    @Override
    public boolean isCommitted(long xid) {
        return true;
    }

    @Override
    public void close() {}
    
}
