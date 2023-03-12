package top.lifeifan.db.backend.dm.logger;

import javafx.util.Pair;
import top.lifeifan.db.backend.utils.FileUtil;
import top.lifeifan.db.backend.utils.Panic;
import top.lifeifan.db.backend.utils.Parser;
import top.lifeifan.db.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author lifeifan
 * @since 2023-02-05
 */
public interface Logger {

    String LOG_SUFFIX = ".log";

    void log(byte[] data);
    void truncate(long x) throws Exception;
    byte[] next();
    void rewind();
    void close();

    static Logger create(String path) {
        File f = new File(path + LOG_SUFFIX);
        FileUtil.createFileCanRW(f);
        Pair<RandomAccessFile, FileChannel> pair = FileUtil.getRafAndChannel(f);
        RandomAccessFile raf = pair.getKey();
        FileChannel fc = pair.getValue();

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new LoggerImpl(raf, fc, 0);
    }

    static Logger open(String path) {
        File f = new File(path + LOG_SUFFIX);
        FileUtil.checkRW(f);
        Pair<RandomAccessFile, FileChannel> pair = FileUtil.getRafAndChannel(f);
        RandomAccessFile raf = pair.getKey();
        FileChannel fc = pair.getValue();

        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();
        return lg;
    }
}
