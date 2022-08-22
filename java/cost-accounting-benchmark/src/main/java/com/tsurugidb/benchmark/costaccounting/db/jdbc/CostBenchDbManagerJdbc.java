package com.tsurugidb.benchmark.costaccounting.db.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.CostMasterDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.FactoryMasterDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.ItemConstructionMasterDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.ItemManufacturingMasterDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.ItemMasterDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.MeasurementMasterDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcDao;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.ResultTableDaoJdbc;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public class CostBenchDbManagerJdbc extends CostBenchDbManager {
    private static final Logger LOG = LoggerFactory.getLogger(CostBenchDbManagerJdbc.class);

    private final List<Connection> connectionList = new CopyOnWriteArrayList<>();
    private final ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<Connection>() {

        @Override
        protected Connection initialValue() {
            Connection c = createConnection();
            connectionList.add(c);
            return c;
        }
    };

    public CostBenchDbManagerJdbc() {
    }

    private Connection createConnection() {
        String url = BenchConst.jdbcUrl();
        String user = BenchConst.jdbcUser();
        String password = BenchConst.jdbcPassword();
        try {
            Connection c = DriverManager.getConnection(url, user, password);
            c.setAutoCommit(false);
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
    protected ResultTableDao newResultTableDao() {
        return new ResultTableDaoJdbc(this);
    }

    @Override
    public void executeDdl(String... sqls) {
        var dao = new JdbcDao<Object>(this, null, null) {
            public void executeDdl() {
                for (String sql : sqls) {
                    try (var ps = preparedStatement(sql)) {
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
        try {
            runnable.run();
            commit();
        } catch (Throwable e) {
            try {
                rollback();
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
            throw e;
        }
    }

    @Override
    public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
        try {
            T r = supplier.get();
            commit();
            return r;
        } catch (Throwable e) {
            try {
                rollback();
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
            throw e;
        }
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
