package com.tsurugidb.benchmark.costaccounting.db.doma2.log;

import java.util.function.Supplier;
import java.util.logging.Level;

import org.seasar.doma.jdbc.UtilLoggingJdbcLogger;

public class MyLoggingJdbcLogger extends UtilLoggingJdbcLogger {

    @Override
    protected void log(Level level, String callerClassName, String callerMethodName, Throwable throwable, Supplier<String> messageSupplier) {
        if (level.intValue() >= Level.WARNING.intValue()) {
            super.log(level, callerClassName, callerMethodName, throwable, messageSupplier);
        }
    }
}
