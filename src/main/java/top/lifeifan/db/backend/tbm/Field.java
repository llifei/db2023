package top.lifeifan.db.backend.tbm;

import com.google.common.primitives.Bytes;
import top.lifeifan.db.backend.im.BPlusTree;
import top.lifeifan.db.backend.parser.statement.SingleExpression;
import top.lifeifan.db.backend.tm.TransactionManagerImpl;
import top.lifeifan.db.backend.utils.Panic;
import top.lifeifan.db.backend.utils.ParseStringRes;
import top.lifeifan.db.backend.utils.Parser;
import top.lifeifan.db.common.OperationFailException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 字段管理
 * [FieldName][TypeName][IndexUid]    如果有索引，则IndexUid指向索引的根，否则该值为0
 *
 * @author lifeifan
 * @since 2023-03-01
 */
public class Field {

    long uid;
    private Table tb;
    String fieldName;
    String fieldType;
    // 如果这个字段有索引，则index指向B+树的根，否则为0
    private long index;
    private BPlusTree bPlusTree;
    private static final Set<String> VALID_FIELD_TYPE
            = Arrays.stream(new String[] {"int32", "int64", "string"}).collect(Collectors.toSet());

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }

    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field field = new Field(tb, fieldName, fieldType, 0);
        if (indexed) {
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            field.index = index;
            field.bPlusTree = bt;
        }
        field.persistSelf(xid);
        return field;
    }

    private static void typeCheck(String fieldType) throws Exception {
        if (!VALID_FIELD_TYPE.contains(fieldType)) {
            throw OperationFailException.InvalidFieldException;
        }
    }

    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;
        position += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        if (index != 0) {
            try {
                bPlusTree = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch (Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }


    public boolean isIndexed() {
        return index != 0;
    }

    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bPlusTree.insert(uKey, uid);
    }

    private long value2Uid(Object key) {
        long uid = 0;
        switch (fieldType) {
            case "string":
                uid = Parser.str2Uid((String)key);
                break;
            case "int32":
                int uint = (int)key;
                return uint;
            case "int64":
                uid = (long)key;
                break;
        }
        return uid;
    }

    public Object string2Value(String str) {
        switch (fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    public byte[] value2Raw(Object value) {
        byte[] raw = null;
        switch (fieldType) {
            case "int32":
                raw = Parser.int2Byte((int) value);
                break;
            case "int64":
                raw = Parser.long2Byte((Long) value);
                break;
            case "string":
                raw = Parser.string2Byte((String) value);
                break;
        }
        return raw;
    }

    public FieldCalRes calExp(SingleExpression exp) {
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch (exp.compareOp) {
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                if (res.right > 0) {
                    res.right--;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }

    public List<Long> search(long l0, long r0) throws Exception {
        return bPlusTree.searchRange(l0, r0);
    }

    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch (fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int62":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    public String printValue(Object value) {
        String str = null;
        switch (fieldType) {
            case "int32":
                str = String.valueOf(value);
                break;
            case "int64":
                str = String.valueOf(value);
                break;
            case "string":
                str = (String) value;
        }
        return str;
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(",")
                .append(fieldType)
                .append(index != 0 ? ", Index" : ", NoIndex")
                .append(")")
                .toString();
    }
}
