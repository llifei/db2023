package top.lifeifan.db.common;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author lifeifan
 * @since 2023-02-02
 */
@Getter
@AllArgsConstructor
public enum Error {

    // common
    FileExistsException("File already exists!"),
    FileCannotRWException("File can't be read or written!"),
    FileNotExistsException("File is not exists!"),
    CacheFullException("Cache is full!"),
    MemTooSmallException("Memory is too small!"),

    // tm,
    BadXIDFileException("Bad XID file!"),

    // dm
    BadLogFileException("Bad Log File!"),
    DataToolLargeException("Data is too large!"),
    DatabaseBusyException("Database is busy now!"),

    // vm
    DeadLockException("Dead lock occurs!"),
    NullEntryException("Entry is null!");

    private final String message;
}
