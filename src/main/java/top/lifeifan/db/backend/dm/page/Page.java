package top.lifeifan.db.backend.dm.page;

/**
 * @author lifeifan
 * @since 2023-02-04
 */
public interface Page {
    void lock();
    void unLock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
