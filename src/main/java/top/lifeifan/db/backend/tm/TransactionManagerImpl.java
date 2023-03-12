package top.lifeifan.db.backend.tm;

import top.lifeifan.db.backend.utils.Panic;
import top.lifeifan.db.backend.utils.Parser;
import top.lifeifan.db.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lifeifan
 * @since 2023-02-03
 */
public class TransactionManagerImpl implements TransactionManager{

    // 每个事务的占用长度
    private static final Integer XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final Byte STATE_TRANSACTION_ACTIVE = 0;
    private static final Byte STATE_TRANSACTION_COMMITED = 1;
    private static final Byte STATE_TRANSACTION_ABORTED = 2;

    // 超级事务XID
    public static final Long SUPER_XID = 0L;

    private RandomAccessFile file;
    private FileChannel fileChannel;
    private Long xidCounter;
    private Lock counterLock;

    TransactionManagerImpl (RandomAccessFile file, FileChannel fileChannel) {
        this.file = file;
        this.fileChannel = fileChannel;
        this.counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查 XID 文件是否合法
     * 读取 XID_FILE_HEADER 中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        Long fileLen = 0L;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }
        if (fileLen < XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_LENGTH);
        try {
            fileChannel.position(0);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        Long end = getXidPosition(this.xidCounter + 1);
        if (!end.equals(fileLen)) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    private Long getXidPosition(long xid) {
        return XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    @Override
    public Long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, STATE_TRANSACTION_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * 将XID加一，并更新XID Header
     */
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 更新xid文件中某事务(xid标识)的状态
     * @param xid 事务标识
     * @param status 事务状态
     */
    private void updateXID(Long xid, Byte status) {
        Long offset = getXidPosition(xid);
        byte[] temp = new byte[XID_FIELD_SIZE];
        temp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(temp);
        try {
            fileChannel.position(offset);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 检查XID事务是否处于status状态
     * @param xid 事务标识id
     * @param status 状态
     * @return tf
     */
    private boolean checkXID(Long xid, Byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    @Override
    public void commit(Long xid) {
        updateXID(xid, STATE_TRANSACTION_COMMITED);
    }

    @Override
    public void abort(Long xid) {
        updateXID(xid, STATE_TRANSACTION_ABORTED);
    }

    @Override
    public Boolean isActive(Long xid) {
        if (xid.equals(SUPER_XID))  return false;
        return checkXID(xid, STATE_TRANSACTION_ACTIVE);
    }

    @Override
    public Boolean isAborted(Long xid) {
        if (xid.equals(SUPER_XID))  return false;
        return checkXID(xid, STATE_TRANSACTION_ABORTED);
    }

    @Override
    public void close() {
        try {
            file.close();
            fileChannel.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 判断tm是否有效
     * @return tf
     */
    @Override
    public Boolean isValid() {
        checkXIDCounter();
        return fileChannel != null && file != null;
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID)   return true;
        return checkXID(xid, STATE_TRANSACTION_COMMITED);
    }
}
