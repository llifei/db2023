package top.lifeifan.db.backend.utils;

import javafx.util.Pair;
import top.lifeifan.db.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class FileUtil {

    public static void createFileCanRW(File f) {
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!f.canWrite() || !f.canRead()) {
            Panic.panic(Error.FileCannotRWException);
        }
    }

    public static Pair<RandomAccessFile, FileChannel> getRafAndChannel(File f) {
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new Pair<>(raf, fc);
    }

    public static void checkRW(File f) {
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
    }
}
