package top.lifeifan.db.common;

public class OperationFailException extends RuntimeException{
    public OperationFailException(String message) {
        super(message);
    }

    public static final Exception ConcurrentUpdateException
            = new RuntimeException("Concurrent update error!");

    public static final Exception InvalidFieldException
            = new RuntimeException("Field Type is Invalid!");

    public static final Exception InvalidValuesException
            = new RuntimeException("Values is invalid!");

    public static final Exception FieldNotIndexedException
            = new RuntimeException("Field is not indexed!");

    public static final Exception FieldNotFoundException
            = new RuntimeException("Field is not found!");

    public static final Exception InvalidLogOpException
            = new RuntimeException("Logic opreator is invalid!");

    public static final Exception DuplicatedTableException
            = new RuntimeException("Table is duplicated!");

    public static final Exception TableNotFoundException
            = new RuntimeException("Table is not found!");

    public static final Exception InvalidCommandException
            = new RuntimeException("Command is invalid");

    public static final Exception TableNoIndexException
            = new RuntimeException("The table have no index!");

    public static final Exception InvalidPkgDataException
            = new RuntimeException("The package data is invalid!");

    public static final Exception NestedTransactionException
            = new RuntimeException("Nested transaction not supported!");

    public static final Exception NoTransactionException
            = new RuntimeException("Not in transaction!");

    public static final Exception InvalidMemException
            = new RuntimeException("Invalid memory!");

    public static final Exception CacheFullException
            = new RuntimeException("Cache is full!");
}
