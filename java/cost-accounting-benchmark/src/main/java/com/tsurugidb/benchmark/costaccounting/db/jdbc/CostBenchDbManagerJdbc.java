/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.costaccounting.db.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.CostMasterDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.FactoryMasterDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.ItemConstructionMasterDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.ItemManufacturingMasterDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.ItemMasterDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcDao;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.MeasurementMasterDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.ResultTableDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.StockHistoryDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

public class CostBenchDbManagerJdbc extends CostBenchDbManager {
    private static final Logger LOG = LoggerFactory.getLogger(CostBenchDbManagerJdbc.class);

    private final String jdbcUrl;
    private final IsolationLevel isolationLevel;

    private final List<Connection> connectionList = new CopyOnWriteArrayList<>();
    private final ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<Connection>() {

        @Override
        protected Connection initialValue() {
            Connection c = createConnection();
            connectionList.add(c);
            return c;
        }
    };

    public CostBenchDbManagerJdbc(DbManagerPurpose purpose, IsolationLevel isolationLevel) {
        super("JDBC", false, purpose);
        this.jdbcUrl = BenchConst.jdbcUrl();
        this.isolationLevel = Objects.requireNonNull(isolationLevel);
        LOG.info("jdbcUrl={}", jdbcUrl);
        LOG.info("isolationLevel={}", isolationLevel);
    }

    private Connection createConnection() {
        String user = BenchConst.jdbcUser();
        String password = BenchConst.jdbcPassword();
        try {
            Connection c = DriverManager.getConnection(jdbcUrl, user, password);
            c.setAutoCommit(false);
            switch (isolationLevel) {
            case READ_COMMITTED:
                c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                break;
            case SERIALIZABLE:
                c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                break;
            default:
                throw new AssertionError(isolationLevel);
            }
            return c;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() {
        if (isSingleTransaction()) {
            if (connectionList.isEmpty()) {
                Connection c = createConnection();
                connectionList.add(c);
            }
            return connectionList.get(0);
        }

        return connectionThreadLocal.get();
    }

    private void removeCurrentTransaction() {
        if (isSingleTransaction()) {
            if (!connectionList.isEmpty()) {
                for (var c : connectionList) {
                    try {
                        c.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            connectionList.clear();
            return;
        }

        try {
            connectionThreadLocal.get().close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            connectionThreadLocal.remove();
        }
    }

    @Override
    protected MeasurementMasterDao newMeasurementMasterDao() {
        return new MeasurementMasterDaoJdbc(this);
    }

    @Override
    protected FactoryMasterDao newFactoryMasterDao() {
        return new FactoryMasterDaoJdbc(this);
    }

    @Override
    protected ItemMasterDao newItemMasterDao() {
        return new ItemMasterDaoJdbc(this);
    }

    @Override
    protected ItemConstructionMasterDao newItemConstructionMasterDao() {
        return new ItemConstructionMasterDaoJdbc(this);
    }

    @Override
    protected ItemManufacturingMasterDao newItemManufacturingMasterDao() {
        return new ItemManufacturingMasterDaoJdbc(this);
    }

    @Override
    protected CostMasterDao newCostMasterDao() {
        return new CostMasterDaoJdbc(this);
    }

    @Override
    protected StockHistoryDao newStockHistoryDao() {
        return new StockHistoryDaoJdbc(this);
    }

    @Override
    protected ResultTableDao newResultTableDao() {
        return new ResultTableDaoJdbc(this);
    }

    @Override
    public void executeDdl(String... sqls) {
        var dao = new JdbcDao<Object>(this, null, null) {
            public void executeDdl() {
                for (String sql : sqls) {
                    var ps = super.preparedStatement(sql);
                    try {
                        ps.execute();
                        commit();
                    } catch (SQLException e) {
                        LOG.info("ddl={}", sql.trim());
                        if (sql.equals(sqls[sqls.length - 1])) {
                            throw new RuntimeException(e);
                        }
                        LOG.warn("execption={}", e.getMessage());
                        rollback();
                    }
                }
            }
        };
        dao.executeDdl();
    }

    @Override
    public void execute(TgTmSetting setting, Runnable runnable) {
        for (;;) {
            try {
                counter.increment(setting, CounterName.BEGIN_TX);
                runnable.run();
                counter.increment(setting, CounterName.TRY_COMMIT);
                commit();
                counter.increment(setting, CounterName.SUCCESS);
                return;
            } catch (Throwable e) {
                counter.increment(setting, CounterName.ABORTED);
                try {
                    rollback();
                } catch (Throwable t) {
                    e.addSuppressed(t);
                }
                if (isRetryable(e)) {
                    removeCurrentTransaction();
                    LOG.info("retry");
                    continue;
                }
                throw e;
            }
        }
    }

    @Override
    public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
        for (;;) {
            try {
                counter.increment(setting, CounterName.BEGIN_TX);
                T r = supplier.get();
                counter.increment(setting, CounterName.TRY_COMMIT);
                commit();
                counter.increment(setting, CounterName.SUCCESS);
                return r;
            } catch (Throwable e) {
                counter.increment(setting, CounterName.ABORTED);
                try {
                    rollback();
                } catch (Throwable t) {
                    e.addSuppressed(t);
                }
                if (isRetryable(e)) {
                    removeCurrentTransaction();
                    LOG.info("retry");
                    continue;
                }
                throw e;
            }
        }
    }

    @Override
    public boolean isRetryable(Throwable t) {
        while (t != null) {
            if (t instanceof SQLException) {
                SQLException se = (SQLException) t;
                boolean ret = isRetryableSQLException(se);
                LOG.debug("caught [{}] retryable exception, ErrorCode = {}, SQLStatus = {}.", se.getMessage(), se.getErrorCode(), se.getSQLState(), se);

                return ret;
            }
            t = t.getCause();
        }
        return false;
    }

    protected boolean isRetryableSQLException(SQLException e) {
        // PostgreSQL
        String sqlState = e.getSQLState();
        if (sqlState != null && sqlState.equals("40001")) {
            // シリアライゼーション失敗
            return true;
        }
        return false;
    }

    @Override
    public void commit(Runnable listener) {
        Connection c = getConnection();
        try {
            c.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (listener != null) {
            listener.run();
        }
    }

    @Override
    public void rollback(Runnable listener) {
        Connection c = getConnection();
        try {
            c.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (listener != null) {
            listener.run();
        }
    }

    private final Map<Connection, Map<String, PreparedStatement>> psMap = new ConcurrentHashMap<>();

    public PreparedStatement preparedStatement(String sql) {
        Connection c = getConnection();
        var map = psMap.computeIfAbsent(c, k -> new ConcurrentHashMap<>());
        return map.computeIfAbsent(sql, k -> {
            try {
                return c.prepareStatement(sql);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void closeConnection() {
        var list = new ArrayList<SQLException>();
        for (var map : psMap.values()) {
            for (var ps : map.values()) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    list.add(e);
                }
            }
        }

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
            for (var e : list) {
                exception.addSuppressed(e);
            }
            throw exception;
        }
        if (!list.isEmpty()) {
            exception = new RuntimeException();
            for (var e : list) {
                exception.addSuppressed(e);
            }
            throw exception;
        }

        psMap.clear();
        connectionList.clear();
    }
}
