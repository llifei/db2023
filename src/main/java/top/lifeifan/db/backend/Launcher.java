package top.lifeifan.db.backend;

import com.google.common.base.Strings;
import org.apache.commons.cli.*;
import top.lifeifan.db.backend.dm.DataManager;
import top.lifeifan.db.backend.server.Server;
import top.lifeifan.db.backend.tbm.TableManager;
import top.lifeifan.db.backend.tm.TransactionManager;
import top.lifeifan.db.backend.utils.Panic;
import top.lifeifan.db.backend.vm.VersionManager;
import top.lifeifan.db.backend.vm.VersionManagerImpl;
import top.lifeifan.db.common.OperationFailException;

/**
 * @author lifeifan
 * @since 2023-02-02
 */
public class Launcher {
    public static final int port = 9999;
    public static final long DEFAULT_MEM = (1 << 20) * 64;
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
//        Options options = new Options();
//        options.addOption("open", true, "-open DBPath");
//        options.addOption("create", true, "-create DBPath");
//        options.addOption("mem", true, "-mem 64MB");
//        CommandLineParser parser = new DefaultParser();
//        CommandLine cmd = parser.parse(options, args);
//
//        if (cmd.hasOption("open")) {
//            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
//            return;
//        }
//        if (cmd.hasOption("create")) {
//            createDB(cmd.getOptionValue("create"));
//            return;
//        }
//        System.out.println("Usage: launcher (open|create) DBPath");
        createDB("C:\\Users\\lifeifan\\Documents\\db2023\\test");
        openDB("C:\\Users\\lifeifan\\Documents\\db2023\\test", DEFAULT_MEM);
    }

    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFAULT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    private static long parseMem(String memStr) {
        if (Strings.isNullOrEmpty(memStr)) {
            return DEFAULT_MEM;
        }
        if (memStr.length() < 2) {
            Panic.panic(OperationFailException.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length() - 2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length() - 2));
        switch (unit) {
            case "KB":
                return memNum * KB;
            case "MB":
                return memNum * MB;
            case "GB":
                return memNum * GB;
            default:
                Panic.panic(OperationFailException.InvalidMemException);
        }
        return DEFAULT_MEM;
    }
}
