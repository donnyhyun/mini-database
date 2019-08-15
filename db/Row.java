package db;

class Row {

    private String[] literals;

    Row(String[] input) {
        literals = input;
        for (int i = 0; i < literals.length; i++) {
            literals[i] = literals[i].trim();
        }
    }

    Row(String input) {
        if (input == "") {
            literals = new String[0];
        } else {
            literals = input.split(",");
            for (int i = 0; i < literals.length; i++) {
                literals[i] = literals[i].trim();
            }
        }
    }

    String[] getLiterals() {
        return literals;
    }

    void add(String e) {
        String[] tmp = new String[literals.length + 1];
        for (int i = 0; i < literals.length; i++) {
            tmp[i] = literals[i];
        }
        tmp[literals.length] = e;
        literals = tmp.clone();
    }

    void add(String[] e) {
        String[] tmp = new String[literals.length + e.length];
        for (int i = 0; i < literals.length; i++) {
            tmp[i] = literals[i];
        }
        for (int i = 0; i < e.length; i++) {
            tmp[literals.length + i] = e[i];
        }
        literals = tmp.clone();
    }

    String get(int index) {
        return literals[index];
    }

    void set(int index, String s) {
        literals[index] = s;
    }

    void print() {
        System.out.print(this);
    }

    protected Row clone() {
        return new Row(literals);
    }

    public String toString() {
        String ret = "";
        for (int i = 0; i < literals.length; i++) {
            ret += literals[i];
            if (i != literals.length - 1) {
                ret += ",";
            }
        }
        return ret;
    }
}
