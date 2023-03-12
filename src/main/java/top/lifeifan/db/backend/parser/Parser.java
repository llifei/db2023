package top.lifeifan.db.backend.parser;

import java.util.ArrayList;
import java.util.List;

import top.lifeifan.db.backend.parser.statement.Abort;
import top.lifeifan.db.backend.parser.statement.Begin;
import top.lifeifan.db.backend.parser.statement.Commit;
import top.lifeifan.db.backend.parser.statement.Create;
import top.lifeifan.db.backend.parser.statement.Delete;
import top.lifeifan.db.backend.parser.statement.Drop;
import top.lifeifan.db.backend.parser.statement.Insert;
import top.lifeifan.db.backend.parser.statement.Select;
import top.lifeifan.db.backend.parser.statement.Show;
import top.lifeifan.db.backend.parser.statement.SingleExpression;
import top.lifeifan.db.backend.parser.statement.Update;
import top.lifeifan.db.backend.parser.statement.Where;
import top.lifeifan.db.common.OperationFailException;

public class Parser {
    public static Object Parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();

        Object stat = null;
        Exception statErr = null;
        try {
            switch (token) {
                case "begin":
                    stat = parseBegin(tokenizer);
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);
                    break;
                case "create":
                    stat = parseCreate(tokenizer);
                    break;
                case  "drop":
                    stat = parseDrop(tokenizer);
                    break;
                case "select":
                    stat = parseSelect(tokenizer);
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);
                    break;
                case "show":
                    stat = parseShow(tokenizer);
                    break;
                default:
                    throw OperationFailException.InvalidCommandException;
            }
        } catch (Exception e) {
            statErr = e;
        }
        try {
            String next = tokenizer.peek();
            if (!"".equals(next)) {
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement" + new String(errStat));
            }
        } catch (Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement" + new String(errStat));
        }
        return stat;
    }

    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            return new Show();
        }
        throw OperationFailException.InvalidCommandException;
    }

    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        update.tableName = tokenizer.peek();
        tokenizer.pop();
        if (!"set".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        tokenizer.pop();

        update.fieldName = tokenizer.peek();
        tokenizer.pop();

        if (!"=".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        tokenizer.pop();

        update.value = tokenizer.peek();
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            update.where = null;
            return update;
        }
        update.where = parseWhere(tokenizer);
        return update;
    }

    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();
        if (!"from".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw OperationFailException.InvalidCommandException;
        }
        delete.tableName = tableName;
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);
        return delete;
    }

    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();

        if (!"into".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw  OperationFailException.InvalidCommandException;
        }
        insert.tableName = tableName;
        tokenizer.pop();

        if (!"values".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }

        List<String> values = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if ("".equals(value)) {
                break;
            }
            values.add(value);
        }
        insert.values = values.toArray(new String[0]);
        return insert;
    }

    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();
        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        if ("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while (true) {
                String field = tokenizer.peek();
                if (!isName(field)) {
                    throw OperationFailException.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if (",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        read.fields = fields.toArray(new String[0]);
        if (!"from".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw OperationFailException.InvalidCommandException;
        }
        read.tableName = tableName;
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            read.where = null;
            return read;
        }
        read.where = parseWhere(tokenizer);
        return read;
    }

    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        if (!"table".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw OperationFailException.InvalidCommandException;
        }
        tokenizer.pop();

        if (!"".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        if (!"table".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        tokenizer.pop();

        Create create = new Create();
        String name = tokenizer.peek();
        if (!isName(name)) {
            throw OperationFailException.InvalidCommandException;
        }
        create.tableName = name;

        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if ("(".equals(field)) {
                break;
            }
            if (!isName(field)) {
                throw OperationFailException.InvalidCommandException;
            }
            tokenizer.pop();
            String fieldType = tokenizer.peek();
            if (!isType(fieldType)) {
                throw OperationFailException.InvalidCommandException;
            }
            fNames.add(field);
            fTypes.add(fieldType);
            tokenizer.pop();

            String next = tokenizer.peek();
            if (",".equals(next)) {
                continue;
            } else if ("".equals(next)) {
                throw OperationFailException.TableNoIndexException;
            } else if ("(".equals(next)) {
                break;
            } else {
                throw OperationFailException.InvalidCommandException;
            }
        }
        create.fieldName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);

        tokenizer.pop();
        if (!"index".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        List<String> indexs = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if (")".equals(field)) {
                break;
            }
            if (!isName(field)) {
                throw OperationFailException.InvalidLogOpException;
            } else {
                indexs.add(field);
            }
        }
        create.index = indexs.toArray(new String[0]);
        tokenizer.pop();

        if (!"".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        return create;
    }

    private static boolean isType(String tp) {
        return ("int32".equals(tp) || "int64".equals(tp) ||
                "string".equals(tp));
    }

    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if (!"".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        return new Abort();
    }

    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        if (!"".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        return new Commit();
    }

    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();
        Begin begin = new Begin();
        if ("".equals(isolation)) {
            return begin;
        }
        if (!"isolation".equals(isolation)) {
            throw OperationFailException.InvalidCommandException;
        }
        tokenizer.pop();
        String level = tokenizer.peek();
        if (!"level".equals(level)) {
            throw OperationFailException.InvalidCommandException;
        }
        tokenizer.pop();

        String tmp1 = tokenizer.peek();
        if ("read".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("committed".equals(tmp2)) {
                tokenizer.pop();
                if (!"".equals(tokenizer.peek())) {
                    throw OperationFailException.InvalidCommandException;
                }
                return begin;
            } else {
                throw OperationFailException.InvalidCommandException;
            }
        } else if ("repeatable".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("read".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if (!"".equals(tokenizer.peek())) {
                    throw OperationFailException.InvalidCommandException;
                }
                return begin;
            } else {
                throw OperationFailException.InvalidCommandException;
            }
        } else {
            throw OperationFailException.InvalidCommandException;
        }
    }

    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();
        if (!"where".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        tokenizer.pop();

        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        String logicOp = tokenizer.peek();
        if ("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        if (!isLogicOp(logicOp)) {
            throw OperationFailException.InvalidCommandException;
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        if (!"".equals(tokenizer.peek())) {
            throw OperationFailException.InvalidCommandException;
        }
        return where;
    }

    private static boolean isLogicOp(String op) {
        return "and".equals(op) || "or".equals(op);
    }

    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();
        String field = tokenizer.peek();
        if (!isName(field)) {
            throw OperationFailException.InvalidCommandException;
        }
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if (!isCmpOp(op)) {
            throw OperationFailException.InvalidCommandException;
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    private static boolean isName(String field) {
        return !(field.length() == 1 && !Tokenizer.isAlphaBeta(field.getBytes()[0]));
    }

    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }
}
