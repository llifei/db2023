package top.lifeifan.db.backend.dm.pageIndex;

import lombok.Data;

/**
 * @author lifeifan
 * @since 2023-02-06
 */
@Data
public class PageInfo {
    private int pgNo;
    private int freeSpace;

    public PageInfo(int pgNo, int freeSpace) {
        this.pgNo = pgNo;
        this.freeSpace = freeSpace;
    }
}
