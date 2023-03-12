package top.lifeifan.db.backend.tm;

import top.lifeifan.db.backend.utils.Panic;
import top.lifeifan.db.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author lifeifan
 * @since 2023-02-03
 */
public interface TransactionManager {

    // XID 文件头长度
    Integer XID_HEADER_LENGTH = 8;

    // XID 文件后缀
    String XID_SUFFIX = ".xid";

    /**
     * 开启新事务
     * @return void
     */
    Long begin();

    /**
     * 提交事务
     * @param xid 事务标识id
     */
    void commit(Long xid);

    /**
     * 取消事务
     * @param xid 事务标识id
     */
    void abort(Long xid);

    /**
     * 查询某事务是否是正在进行状态
     * @param xid 事务标识id
     * @return is Active
     */
    Boolean isActive(Long xid);

    /**
     * 查询某事务是否是已取消状态
     * @param xid 事务标识id
     * @return is Aborted
     */
    Boolean isAborted(Long xid);

    /**
     * 判断tm是否有效
     * @return tf
     */
    Boolean isValid();

    boolean isCommitted(long xid);

    /**
     * 关闭事务管理器TM
     */
    void close();

    static TransactionManagerImpl create(String path) {
        File xidFile = new File(path + XID_SUFFIX);
        try {
            if (!xidFile.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!xidFile.canWrite() || !xidFile.canRead()) {
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(xidFile, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_HEADER_LENGTH]);
        try {
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(randomAccessFile, fileChannel);
    }

    static TransactionManagerImpl open(String path) {
        File xidFile = new File(path + XID_SUFFIX);
        if (!xidFile.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!xidFile.canRead() || !xidFile.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(xidFile, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(randomAccessFile, fileChannel);
    }
}
