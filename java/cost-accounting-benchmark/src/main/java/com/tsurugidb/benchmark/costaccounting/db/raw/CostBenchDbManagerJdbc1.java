package com.tsurugidb.benchmark.costaccounting.db.raw;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.tsurugidb.benchmark.costaccounting.BenchConst;

public class CostBenchDbManagerJdbc1 extends CostBenchDbManagerJdbc {

    private final List<Connection> connectionList = new CopyOnWriteArrayList<>();

    private final ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<Connection>() {

        @Override
        protected Connection initialValue() {
            try {
                Connection c = createConnection();
                connectionList.add(c);
                return c;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    };

    public CostBenchDbManagerJdbc1() {
    }

    private Connection createConnection() throws SQLException {
        String url = BenchConst.jdbcUrl();
        String user = BenchConst.jdbcUser();
        String password = BenchConst.jdbcPassword();
        Connection c = DriverManager.getConnection(url, user, password);

        c.setAutoCommit(false);

        return c;
    }

    @Override
    public Connection getConnection() {
        return connectionThreadLocal.get();
    }

    @Override
    public void commit() {
        Connection c = getConnection();
        try {
            c.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rollback() {
        Connection c = getConnection();
        try {
            c.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        RuntimeException exception = null;

        for (Connection c : connectionList) {
            try {
                c.close();
            } catch (SQLException e) {
                if (exception == null) {
                    exception = new RuntimeException(e);
                } else {
                    exception.addSuppressed(e);
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
    }
}
