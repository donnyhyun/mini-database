package db;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Database {

    private HashMap<String, Table> tables;

    // Various common constructs, simplifies parsing.
    private static final String REST = "\\s*(.*)\\s*", COMMA = "\\s*,\\s*", AND = "\\s+and\\s+";

    // Stage 1 syntax, contains the command name.
    private static final Pattern CREATE_CMD = Pattern.compile("create table " + REST),
            LOAD_CMD = Pattern.compile("load " + REST),
            STORE_CMD = Pattern.compile("store " + REST),
            DROP_CMD = Pattern.compile("drop table " + REST),
            INSERT_CMD = Pattern.compile("insert into " + REST),
            PRINT_CMD = Pattern.compile("print " + REST),
            SELECT_CMD = Pattern.compile("select " + REST);

    // Stage 2 syntax, contains the clauses of commands.
    private static final Pattern CREATE_NEW = Pattern
            .compile("(\\S+)\\s+\\(\\s*(\\S+\\s+\\S+\\s*" + "(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)"),
            SELECT_CLS = Pattern
                    .compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+"
                            + "(\\S+\\s*(?:,\\s*\\S+\\s*)*)(?:\\s+where\\s+"
                            + "([\\w\\s+\\-*/'<>=!.]+?(?:\\s+and\\s+"
                            + "[\\w\\s+\\-*/'<>=!.]+?)*))?"),
            CREATE_SEL = Pattern.compile("(\\S+)\\s+as select\\s+" + SELECT_CLS.pattern()),
            INSERT_CLS = Pattern.compile("(\\S+)\\s+values\\s+(.+?" + "\\s*(?:,\\s*.+?\\s*)*)");

    public Database() {
        tables = new HashMap<String, Table>();
    }

    public static void main(String[] args) {
        Database db = new Database();

        db.transact("load t1");
        db.transact("print t1");
        db.transact("load t2");
        db.transact("print t2");
        db.transact("load t4");
        db.transact("print t4");
        db.transact("create table t3 as select * from t1, t2");
        db.transact("create table t5 as select * from t3, t4");
        db.transact("print t3");
        db.transact("print t5");
    }

    public String transact(String query) {
        Matcher m;
        try {
            if ((m = CREATE_CMD.matcher(query)).matches()) {
                return createTable(m.group(1));
            } else if ((m = LOAD_CMD.matcher(query)).matches()) {
                return load(m.group(1));
            } else if ((m = STORE_CMD.matcher(query)).matches()) {
                return store(m.group(1));
            } else if ((m = DROP_CMD.matcher(query)).matches()) {
                return dropTable(m.group(1));
            } else if ((m = INSERT_CMD.matcher(query)).matches()) {
                return insertRow(m.group(1));
            } else if ((m = PRINT_CMD.matcher(query)).matches()) {
                return print(m.group(1));
            } else if ((m = SELECT_CMD.matcher(query)).matches()) {
                return select(m.group(1));
            } else {
                return "ERROR: Malformed query: " + query + "\n";
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            return "ERROR: " + e.getMessage();
        }
    }

    private String createTable(String expr) {
        Matcher m;
        if ((m = CREATE_NEW.matcher(expr)).matches()) {
            return createNewTable(m.group(1), m.group(2).split(COMMA));
        } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
            return createSelectedTable(m.group(1), m.group(2), m.group(3), m.group(4));
        } else {
            return "ERROR: Malformed create: " + expr + "\n";
        }
    }

    private String createNewTable(String name, String[] cols) {
        name = name.trim();
        Table t = new Table(cols);
        tables.put(name, t);
        return "";
    }

    private String createSelectedTable(String name, String exprs, String tbls, String conds) {
        name = name.trim();
        if (exprs == null) {
            exprs = "";
        }
        if (tbls == null) {
            tbls = "";
        }
        if (conds == null) {
            conds = "";
        }
        String[] expressions = exprs.split(COMMA);
        String[] tableNames = tbls.split(COMMA);
        String[] conditions = conds.split(AND);
        Table t = null;
        for (int i = 0; i < tableNames.length; i++) {
            tableNames[i] = tableNames[i].trim();
            if (t == null) {
                t = this.tables.get(tableNames[i]);
            } else {
                t = t.merge(this.tables.get(tableNames[i]));
            }
        }
        for (int k = 0; k < conditions.length; k++) {
            conditions[k] = conditions[k].trim();
            t = t.select(expressions, conditions[k]);
        }
        this.tables.put(name, t);
        return "";
    }

    private String load(String name) {
        name = name.trim();
        try {
            BufferedReader br = new BufferedReader(new FileReader(name + ".tbl"));
            String header = br.readLine();
            if (header == null) {
                br.close();
                return "ERROR: missing header";
            }
            String[] columns = header.split(COMMA);
            Table table = new Table(columns);
            String row;
            while ((row = br.readLine()) != null) {
                row = row.trim();
                if (row.equals("")) {
                    break;
                }
                table.insert(new Row(row.split(COMMA)));
            }
            tables.put(name, table);
            br.close();
        } catch (FileNotFoundException e) {
            return "ERROR: couldn't find table " + name + " (LOAD).";
        } catch (IOException e) {
            return "ERROR: ??";
        }
        return "";
    }

    private String store(String name) {
        name = name.trim();
        try {
            if (!tables.containsKey(name)) {
                return "ERROR: table does not exist.";
            }
            PrintStream pr = new PrintStream(name + ".tbl");
            if (tables.get(name) == null) {
                pr.close();
                return "ERROR: no table to store";
            }
            pr.println(tables.get(name));
            pr.close();
        } catch (FileNotFoundException e) {
            return "ERROR: couldn't find table (STORE).";
        }
        return "";
    }

    private String dropTable(String name) {
        name = name.trim();
        if (tables.get(name) == null) {
            return "ERROR: no table to drop";
        }

        tables.remove(name);
        return "";
    }

    private String insertRow(String expr) {
        Matcher m = INSERT_CLS.matcher(expr);
        if (!m.matches()) {
            return "ERROR: Malformed insert: " + expr + "\n";
        }
        String tableName = m.group(1);
        String row = m.group(2);
        return tables.get(tableName).insert(new Row(row));
    }

    private String print(String name) {
        name = name.trim();
        Table t = tables.get(name);
        if (t == null) {
            return "ERROR: " + name + " table doesn't exist";
        } else {
            return t.toString();
        }
    }

    private String select(String expr) {
        Matcher m = SELECT_CLS.matcher(expr);
        if (!m.matches()) {
            return "ERROR: Malformed select: " + expr + "\n";
        }
        return select(m.group(1), m.group(2), m.group(3));
    }

    private String select(String exprs, String tbls, String conds) {
        if (exprs == null) {
            exprs = "";
        }
        if (tbls == null) {
            tbls = "";
        }
        if (conds == null) {
            conds = "";
        }
        String[] expressions = exprs.split(COMMA);
        String[] tableNames = tbls.split(COMMA);
        String[] conditions = conds.split(AND);
        Table t = null;
        for (int i = 0; i < tableNames.length; i++) {
            if (t == null) {
                t = this.tables.get(tableNames[i]);
            } else {
                t = t.merge(this.tables.get(tableNames[i]));
            }
        }
        for (int k = 0; k < conditions.length; k++) {
            t = t.select(expressions, conditions[k]);
        }
        return t.toString();
    }
}
