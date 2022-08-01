package com.tsurugidb.benchmark.costaccounting.db.raw;

import java.sql.Connection;
import java.sql.SQLException;

import org.seasar.doma.jdbc.tx.LocalTransaction;
import org.seasar.doma.jdbc.tx.LocalTransactionDataSource;

import com.tsurugidb.benchmark.costaccounting.db.doma2.config.AppConfig;

public class CostBenchDbManagerJdbc2 extends CostBenchDbManagerJdbc {

    private final LocalTransaction transaction;

    public CostBenchDbManagerJdbc2() {
        LocalTransactionDataSource dataSource = AppConfig.singleton().getDataSource();
        this.transaction = dataSource.getLocalTransaction(AppConfig.singleton().getJdbcLogger());
    }

    @Override
    public Connection getConnection() {
        if (!transaction.isActive()) {
            transaction.begin();
        }

        LocalTransactionDataSource dataSource = AppConfig.singleton().getDataSource();
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commit(Runnable listener) {
        if (transaction.isActive()) {
            transaction.commit();
            if (listener != null) {
                listener.run();
            }
        }
    }

    @Override
    public void rollback(Runnable listener) {
        if (transaction.isActive()) {
            transaction.rollback();
            if (listener != null) {
                listener.run();
            }
        }
    }

    @Override
    public void close() {
        // do nothing
    }
}
