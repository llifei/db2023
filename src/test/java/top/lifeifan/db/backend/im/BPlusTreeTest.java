package top.lifeifan.db.backend.im;

import org.junit.Test;
import top.lifeifan.db.backend.dm.DataManager;
import top.lifeifan.db.backend.dm.pageCache.PageCache;
import top.lifeifan.db.backend.tm.MockTransactionManager;
import top.lifeifan.db.backend.tm.TransactionManager;

import java.io.File;
import java.util.List;

public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        try {
            TransactionManager tm = new MockTransactionManager();
            DataManager dm = DataManager.create("D:\\Projects\\MYDB\\TestTreeSingle", PageCache.PAGE_SIZE * 10, tm);

            long root = BPlusTree.create(dm);
            BPlusTree tree = BPlusTree.load(root, dm);

            int lim = 10000;
            for (int i = lim - 1; i >= 0; i--) {
                tree.insert(i, i);
            }

            for (int i = 0; i < lim; i++) {
                List<Long> uids = tree.search(i);
                assert uids.size() == 1;
                assert uids.get(0) == i;
            }
        } finally {
            assert new File("D:\\Projects\\MYDB\\TestTreeSingle.db").delete();
            assert new File("D:\\Projects\\MYDB\\TestTreeSingle.log").delete();
        }

    }
}
