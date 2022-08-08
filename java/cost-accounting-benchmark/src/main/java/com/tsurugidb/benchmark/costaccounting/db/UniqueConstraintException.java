package com.tsurugidb.benchmark.costaccounting.db;

@SuppressWarnings("serial")
public class UniqueConstraintException extends RuntimeException {

    public UniqueConstraintException(Throwable cause) {
        super(cause);
    }
}
