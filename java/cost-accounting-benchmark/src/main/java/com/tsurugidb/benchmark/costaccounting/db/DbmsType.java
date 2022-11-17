package com.tsurugidb.benchmark.costaccounting.db;

public enum DbmsType {
    ORACLE, POSTGRESQL, TSURUGI;

    public static DbmsType of(String type) {
        switch (type.toLowerCase()) {
        case "oracle":
            return DbmsType.ORACLE;
        case "postgresql":
            return DbmsType.POSTGRESQL;
        case "tsurugi":
            return DbmsType.TSURUGI;
        default:
            throw new UnsupportedOperationException("unsupported dbms.type=" + type);
        }
    }
}
