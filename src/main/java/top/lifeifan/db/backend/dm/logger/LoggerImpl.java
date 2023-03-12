package top.lifeifan.db.backend.dm.logger;

import com.google.common.primitives.Bytes;
import top.lifeifan.db.backend.utils.Panic;
import top.lifeifan.db.backend.utils.Parser;
import top.lifeifan.db.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件读写
 *
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 *
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 *
 * @author lifeifan
 * @since 2023-02-05
 */
public class LoggerImpl implements Logger{

    private static final int SEED = 13331;
    private static final int OF_SIZE = 0;
    private static final int LEN_SIZE_OR_CHECKSUM = 4;
    private static final int OF_CHECKSUM = OF_SIZE + LEN_SIZE_OR_CHECKSUM;
    private static final int OF_DATA = OF_CHECKSUM + LEN_SIZE_OR_CHECKSUM;

    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock lock;

    private long position;
    private long fileSize;
    private int xCheckNum;


    public LoggerImpl(RandomAccessFile raf, FileChannel fc, int xCheckNum) {
        this.raf = raf;
        this.fc = fc;
        this.xCheckNum = xCheckNum;
        lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    void init() {
        long size = 0;
        try {
            size = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        // log文件不能小于4字节
        if (size < 4) {
            Panic.panic(Error.BadLogFileException);
        }
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xCheckNum = Parser.parseInt(raw.array());
        this.fileSize = size;

        checkAndRemoveTail();
    }

    /**
     * 校验日志文件的 xCheckSum，并移除尾部可能存在的 BadTail
     */
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while(true) {
            byte[] log = internNext();
            if (log == null) {
                break;
            }
            xCheck = calCheckSum(xCheck, log);
        }
        if (xCheck != xCheckNum) {
            Panic.panic(Error.BadLogFileException);
        }

        // 截断文件到正常日志的末尾（去掉异常日志）
        try {
            truncate(position);
            // 为下一条日志设置开始的位置
            raf.seek(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        rewind();
    }

    private int calCheckSum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    private byte[] internNext() {
        if (position + OF_DATA >= fileSize) {
            return null;
        }

        // 一条日志的格式：[Size(4B)] [CheckSum(4B)] [Data]
        // 读取size
        ByteBuffer buf = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(buf.array());
        if (position + OF_DATA + size > fileSize) {
            return null;
        }

        // 读取 checkSum + data
        // allocate(OF_DATA + size)  OF_DATA 代指头部：size + checkSum 两个字段的大小
        buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 校验 checkSum
        byte[] log = buf.array();
        int checkSum1 = calCheckSum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checkSum1 != checkSum2) {
            return null;
        }

        position += log.length;
        return log;
    }

    @Override
    public void log(byte[] data) {

        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXCheckSum(log);
    }

    private void updateXCheckSum(byte[] log) {
        this.xCheckNum = calCheckSum(this.xCheckNum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(this.xCheckNum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calCheckSum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) {
                return null;
            }
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = OF_SIZE + LEN_SIZE_OR_CHECKSUM;
    }

    @Override
    public void close() {
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
