package top.lifeifan.db.backend.tbm;

import top.lifeifan.db.backend.dm.DataManager;
import top.lifeifan.db.backend.parser.statement.*;
import top.lifeifan.db.backend.utils.Parser;
import top.lifeifan.db.backend.vm.VersionManager;

/**
 *表管理
 *  基于 VersionManager
 * @author lifeifan
 * @since 2023-03-01
 */
public interface TableManager {

    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid);
    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0L));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
