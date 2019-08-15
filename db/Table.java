package db;

import java.util.ArrayList;
import java.util.Iterator;

class Table implements Iterable<Row> {

    private String[] columns;
    private String[] column_types;
    private ArrayList<Row> rows;

    Table(String[] input) {
        columns = new String[input.length];
        column_types = new String[input.length];
        for (int i = 0; i < input.length; i++) {
            String[] tmp = input[i].split("\\s+");
            if (tmp.length == 2) {
                columns[i] = tmp[0];
                if (tmp[1].equals("string") || tmp[1].equals("int")
                        || tmp[1].equals("float")) {
                    column_types[i] = tmp[1];
                } else {
                    throw new RuntimeException("ERROR: Invalid column type.");
                }
            } else {
                throw new RuntimeException("ERROR: table constructor fails.");
            }
        }
        rows = new ArrayList<>();
    }

    String insert(Row row) {
        String[] literals = row.getLiterals();
        for (int i = 0; i < literals.length; i++) {
            if (literals[i].equals("NOVALUE")) {
                continue;
            }
            String type;
            if (literals[i].contains("'")) {
                type = "string";
            } else if (literals[i].contains(".")) {
                type = "float";
                row.set(i, String.format("%.3f", Float.parseFloat(literals[i])));
            } else {
                type = "int";
            }
            if (!type.equals(column_types[i])) {
                return "ERROR: row does not match the given table";
            }
        }
        rows.add(row);
        return "";
    }

    Table merge(Table t) {
        String[] columns_new = new String[columns.length + t.columns.length];
        String[] column_types_new = new String[column_types.length + t.column_types.length];
        for (int i = 0; i < columns.length; i++) {
            columns_new[i] = columns[i];
            column_types_new[i] = column_types[i];
        }
        for (int i = 0; i < t.columns.length; i++) {
            columns_new[columns.length + i] = t.columns[i];
            column_types_new[columns.length + i] = t.column_types[i];
        }

        boolean same = false;
        ArrayList<Integer> same_cols_ind_this = new ArrayList<Integer>();
        ArrayList<Integer> same_cols_ind_t = new ArrayList<Integer>();
        for (int i = 0; i < this.columns.length; i++) {
            for (int j = 0; j < t.columns.length; j++) {
                if (columns[i].equals(t.columns[j])) {
                    same_cols_ind_this.add(i);
                    same_cols_ind_t.add(j);
                    same = true;
                }
            }
        }

        ArrayList<Row> rows = new ArrayList<>();
        String[] header;
        if (same) {
            header = new String[this.columns.length + t.columns.length - same_cols_ind_t.size()];
            for (int i = 0; i < this.columns.length; i++) {
                header[i] = this.columns[i] + " " + this.column_types[i];
            }
            int index = 0;
            for (int i = 0; i < t.columns.length; i++) {
                boolean skip = false;
                for (int j = 0; j < same_cols_ind_t.size(); j++) {
                    if (i == same_cols_ind_t.get(j)) {
                        skip = true;
                        break;
                    }
                }
                if (skip)
                    continue;
                header[this.columns.length + index] = t.columns[i] + " " + t.column_types[i];
                index++;
            }

            for (int i = 0; i < this.rows.size(); i++) {
                for (int j = 0; j < t.rows.size(); j++) {
                    boolean equiv = true;
                    for (int k = 0; k < same_cols_ind_this.size(); k++) {
                        String a = this.rows.get(i).get(same_cols_ind_this.get(k));
                        String b = t.rows.get(j).get(same_cols_ind_t.get(k));
                        if (!a.equals(b)) {
                            equiv = false;
                            break;
                        }
                    }
                    if (!equiv) {
                        continue;
                    }
                    Row r = this.rows.get(i).clone();
                    for (int k = 0; k < t.columns.length; k++) {
                        boolean skip = false;
                        for (int l = 0; l < same_cols_ind_t.size(); l++) {
                            if (k == same_cols_ind_t.get(l)) {
                                skip = true;
                                break;
                            }
                        }
                        if (skip)
                            continue;
                        r.add(t.rows.get(j).get(k));
                    }
                    rows.add(r);
                }
            }
        } else {
            header = new String[columns_new.length];
            for (int i = 0; i < columns_new.length; i++) {
                header[i] = columns_new[i] + " " + column_types_new[i];
            }
            for (int i = 0; i < this.rows.size(); i++) {
                for (int j = 0; j < t.rows.size(); j++) {
                    Row r = new Row("");
                    r.add(this.rows.get(i).getLiterals());
                    r.add(t.rows.get(j).getLiterals());
                    rows.add(r);
                }
            }
        }

        Table ret = new Table(header);
        ret.rows = rows;

        return ret;
    }

    private class Info {
        String[] col;
        String[] type;
        ArrayList<Row> rows;

        @SuppressWarnings("unchecked")
        Info(String[] col, String[] type, ArrayList<Row> rows) {
            this.col = col.clone();
            this.type = type.clone();
            this.rows = (ArrayList<Row>) rows.clone();
        }

        @SuppressWarnings("unchecked")
        Info(String col, String type, ArrayList<Row> rows) {
            this.col = new String[1];
            this.type = new String[1];
            this.col[0] = col;
            this.type[0] = type;
            this.rows = (ArrayList<Row>) rows.clone();
        }
    }

    Table select(String[] exprs, String cond) {
        String header = "";
        ArrayList<Row> rows = null;
        for (int i = 0; i < exprs.length; i++) {
            Info info = select_helper(exprs[i].trim(), cond);
            String tmp = "";
            for (int j = 0; j < info.col.length; j ++) {
                tmp += info.col[j] + " " + info.type[j];
                if (j != info.col.length - 1) {
                    tmp += ",";
                }
            }
            header += tmp;
            if (i != exprs.length - 1) {
                header += ",";
            }
            if (rows == null) {
                rows = info.rows;
            } else {
                for (int j = 0; j < rows.size(); j++) {
                    rows.get(j).add(info.rows.get(j).getLiterals());
                }
            }
        }
        Table t = new Table(header.split(","));
        t.rows = rows;
        return t;
    }

    private Info select_helper(String expr, String cond) {
        expr = expr.trim();
        ArrayList<Row> results = select_helper(cond);
        ArrayList<Row> ret = new ArrayList<>();
        if (expr.equals("*")) {
            return new Info(columns, column_types, results);
        }
        if (expr.equals(".*")) {
            throw new RuntimeException("ERROR: invalid operator");
        }

        int index = -1;
        // run only if expr is a single column name
        for (int i = 0; i < columns.length; i++) {
            if (expr.equals(columns[i])) {
                index = i;
                break;
            }
        }
        if (index != -1) {
            for (Row r : results) {
                ret.add(new Row(r.getLiterals()[index]));
            }
            return new Info(columns[index], column_types[index], ret);
        }
        // Finds the operator
        String operator = "+";
        int operator_index = expr.indexOf(operator);
        if (operator_index == -1) {
            operator = "-";
            operator_index = expr.indexOf(operator);
        }
        if (operator_index == -1) {
            operator = "*";
            operator_index = expr.indexOf(operator);
        }
        if (operator_index == -1) {
            operator = "/";
            operator_index = expr.indexOf(operator);
        }
        int end_index = expr.indexOf(" as ");
        if (end_index == -1) {
            end_index = expr.length() - 1;
        }
        String op1 = expr.substring(0, operator_index);
        String op2 = expr.substring(operator_index + 1, end_index + 1);
        String alias = expr.substring(end_index + 3).trim();
        op1 = op1.trim();
        op2 = op2.trim();
        int op1_ind = -1;
        int op2_ind = -1;
        for (int i = 0; i < columns.length; i++) {
            if (op2.equals(columns[i])) {
                op2_ind = i;
                break;
            }
        }
        for (int i = 0; i < columns.length; i++) {
            if (op1.equals(columns[i])) {
                op1_ind = i;
                break;
            }
        }
        String op1_type = column_types[op1_ind];
        String op2_type = "string";
        float op2_float = 0;
        int op2_int = 0;
        if (op2_ind == -1) {
            try {
                op2_float = Float.parseFloat(op2);
                op2_type = "float";
            } catch (NumberFormatException e) {
                op2_type = "string";
            }
            if ((int)op2_float == op2_float) {
                op2_int = (int)op2_float;
                op2_type = "int";
            }
        } else {
            op2_type = column_types[op2_ind];
        }

        String type = "string";
        for (Row r : results) {
            if ((op1_type.equals("string") && !op2_type.equals("string")) ||
                    (!op1_type.equals("string") && op2_type.equals("string"))) {
                throw new RuntimeException("ERROR: can't compute string and non-string");
            }
            op1 = r.getLiterals()[op1_ind];
            if (op2_ind != -1) {
                op2 = r.getLiterals()[op2_ind];
            }
            if (op1_type.equals("string") && op2_type.equals("string")) {
                if (operator.equals("+")) {
                    op1 = op1.substring(0, op1.length() - 1);
                    op2 = op2.substring(1);
                }
                ret.add(new Row(op1 + op2));
                type = "string";
            } else if (op1_type.equals("int") && op2_type.equals("int")) {
                int op1_int = Integer.parseInt(op1);
                if (op2_ind != -1) {
                    op2_int = Integer.parseInt(op2);
                }
                int result = 0;
                if (operator.equals("+")) {
                    result = op1_int + op2_int;
                } else if (operator.equals("-")) {
                    result = op1_int - op2_int;
                } else if (operator.equals("*")) {
                    result = op1_int * op2_int;
                } else if (operator.equals("/")) {
                    result = op1_int / op2_int;
                }
                ret.add(new Row(Integer.toString(result)));
                type = "int";
            } else {
                float op1_float = Float.parseFloat(op1);
                if (op2_ind != -1) {
                    op2_float = Float.parseFloat(op2);
                }
                float result = 0;
                if (operator.equals("+")) {
                    result = op1_float + op2_float;
                } else if (operator.equals("-")) {
                    result = op1_float - op2_float;
                } else if (operator.equals("*")) {
                    result = op1_float * op2_float;
                } else if (operator.equals("/")) {
                    result = op1_float / op2_float;
                }
                ret.add(new Row(String.format("%.3f", result)));
                type = "float";
            }
        }
        return new Info(alias, type, ret);
    }

    private ArrayList<Row> select_helper(String cond) {
        if (cond == "") {
            return rows;
        }
        String operator = "==";
        int operator_index = cond.indexOf(operator);
        if (operator_index == -1) {
            operator = "!=";
            operator_index = cond.indexOf(operator);
        }
        if (operator_index == -1) {
            operator = "<=";
            operator_index = cond.indexOf(operator);
        }
        if (operator_index == -1) {
            operator = ">=";
            operator_index = cond.indexOf(operator);
        }
        if (operator_index == -1) {
            operator = "<";
            operator_index = cond.indexOf(operator);
        }
        if (operator_index == -1) {
            operator = ">";
            operator_index = cond.indexOf(operator);
        }

        String col1 = cond.substring(0, operator_index);
        String col2 = cond.substring(operator_index + operator.length());
        col1 = col1.trim();
        col2 = col2.trim();
        int col1_ind = -1;
        int col2_ind = -1;
        boolean unary = true;

        for (int i = 0; i < columns.length; i++) {
            if (col2.equals(columns[i])) {
                col2_ind = i;
                unary = false;
                break;
            }
        }
        for (int i = 0; i < columns.length; i++) {
            if (col1.equals(columns[i])) {
                col1_ind = i;
                break;
            }
        }

        ArrayList<Row> ret = new ArrayList<>();
        int ijk = 0;
        while (ijk < rows.size()) {
            col1 = rows.get(ijk).getLiterals()[col1_ind];
            String col1_type = column_types[col1_ind];
            if (!unary) {
                col2 = rows.get(ijk).getLiterals()[col2_ind];
                String col2_type = column_types[col2_ind];
                if ((col1_type.equals("string") && !col2_type.equals("string")) ||
                        (!col1_type.equals("string") && col2_type.equals("string"))) {
                    throw new RuntimeException("ERROR: can't compare string and non-string");
                }
            }

            boolean result;
            if (col1_type.equals("string")) {
                result = compare(col1, operator, col2);
            } else {
                result = compare(Float.parseFloat(col1), operator, Float.parseFloat(col2));
            }
            if (result) {
                ret.add(rows.get(ijk));
            }
            ijk += 1;
        }
        return ret;
    }

    private boolean compare(String col1, String operator, String col2) {
        if (operator.equals("==")) {
            if (col1.equals(col2)) {
                return true;
            }
        } else if (operator.equals("!=")) {
            if (!col1.equals(col2)) {
                return true;
            }
        } else if (operator.equals("<=")) {
            if (col1.compareTo(col2) <= 0)  {
                return true;
            }
        } else if (operator.equals(">=")) {
            if (col1.compareTo(col2) >= 0) {
                return true;
            }
        } else if (operator.equals("<")) {
            if (col1.compareTo(col2) < 0) {
                return true;
            }
        } else if (operator.equals(">")) {
            if (col1.compareTo(col2) > 0) {
                return true;
            }
        }
        return false;
    }

    private boolean compare(float col1, String operator, float col2) {
        if (operator.equals("==")) {
            if (col1 == col2) {
                return true;
            }
        } else if (operator.equals("!=")) {
            if (col1 != col2) {
                return true;
            }
        } else if (operator.equals("<=")) {
            if (col1 <= col2) {
                return true;
            }
        } else if (operator.equals(">=")) {
            if (col1 >= col2) {
                return true;
            }
        } else if (operator.equals("<")) {
            if (col1 < col2) {
                return true;
            }
        } else if (operator.equals(">")) {
            if (col1 > col2) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<Row> iterator() {
        return rows.iterator();
    }

    public String toString() {
        String ret = "";
        for (int i = 0; i < columns.length; i++) {
            ret += columns[i] + " " + column_types[i];
            if (i != columns.length - 1) {
                ret += ",";
            }
        }
        for (Row row : rows) {
            ret += "\n";
            ret += row;
        }
        return ret;
    }
}
