package com.tsurugidb.benchmark.costaccounting.debug;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager.DbManagerPurpose;
import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.debug.iceaxe_dbtest.DebugInsertDuplicate;
import com.tsurugidb.benchmark.costaccounting.debug.iceaxe_dbtest.DebugSelectFetch;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;

public class DebugCommand implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(DebugCommand.class);

    @Override
    public String getDescription() {
        return "Execute debug. args={command}";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        if (args.length <= 1) {
            System.err.println("ERROR: too few arguments.");
            System.err.println(getDescription());
            return 1;
        }

        String type = args[1];
        switch (type) {
        case "tsubakuro":
            tsubakuro();
            break;
        case "manager":
            debugManager();
            break;
        case "manager2":
            debugManager2();
            break;
        case "session":
            sessionLimit();
            break;
        case "insert_dup":
            new DebugInsertDuplicate(args).execute();
            break;
        case "fetch":
            new DebugSelectFetch(args).execute();
            break;
        case "select-cost":
            selectCost(arg(args, 2, "txOption"));
            break;
        case "insert-stock":
            insertStock(arg(args, 2, "txOption"), argInt(args, 3, "recordSize"));
            break;
        case "copy-stock":
            copyStock(arg(args, 2, "txOption"));
            break;
        case "copy-stock.list":
            copyStockList(arg(args, 2, "txOption"));
            break;
        default:
            throw new UnsupportedOperationException("unsupported operation. type=" + type);
        }

        return 0;
    }

    private String arg(String[] args, int index, String name) {
        if (index < args.length) {
            return args[index];
        }
        throw new RuntimeException("too few arguments. " + name);
    }

    private int argInt(String[] args, int index, String name) {
        if (index < args.length) {
            return Integer.parseInt(args[index]);
        }
        throw new RuntimeException("too few arguments. " + name);
    }

    private void tsubakuro() {
        var endpoint = BenchConst.tsurugiEndpoint();
        LOG.info("endpoint={}", endpoint);
        var connector = Connector.create(endpoint);

        var sessionList = new CopyOnWriteArrayList<Session>();
        var threadList = new ArrayList<Thread>();
        var alive = new AtomicBoolean(true);
        for (int i = 0; i < 60; i++) {
            var thread = new Thread(() -> {
                Session session;
                try {
                    session = SessionBuilder.connect(connector).create();
                } catch (Exception e) {
                    LOG.warn("connect error. {}: {}", e.getClass().getName(), e.getMessage());
                    return;
                }
                LOG.info("session created. {}", session);
                sessionList.add(session);

                while (alive.get()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            threadList.add(thread);
            thread.start();
        }

        alive.set(false);

        LOG.info("thread join start");
        for (var thread : threadList) {
            try {
                thread.join();
            } catch (Exception e) {
                LOG.warn("join error", e);
            }
        }
        LOG.info("thread join end");

        LOG.info("close session start");
        for (var session : sessionList) {
            try {
                session.close();
            } catch (Exception e) {
                LOG.warn("close error", e);
            }
        }
        LOG.info("close session end");
    }

    private void debugManager() {
        try (var dbManager = (CostBenchDbManagerIceaxe) createDbManager()) {
            LOG.info("thread start");

            var threadList = new ArrayList<Thread>();
            var alive = new AtomicBoolean(true);
            for (int i = 0; i < 60; i++) {
                var thread = new Thread(() -> {
                    var session = dbManager.getSession();
                    try {
                        session.getLowSqlClient();
                    } catch (IOException e) {
                        LOG.warn("sqlClient error", e);
                        throw new UncheckedIOException(e.getMessage(), e);
                    } catch (InterruptedException e) {
                        LOG.warn("sqlClient error", e);
                        throw new RuntimeException(e);
                    }
                    while (alive.get()) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                threadList.add(thread);
                thread.start();
            }

            alive.set(false);

            LOG.info("thread join start");
            for (var thread : threadList) {
                try {
                    thread.join();
                } catch (Exception e) {
                    LOG.warn("join error", e);
                }
            }
            LOG.info("thread join end");
        }
    }

    private void debugManager2() {
        try (var dbManager = createDbManager()) {
            LOG.info("thread start");
            var setting = TgTmSetting.of(TgTxOption.ofLTX(ResultTableDao.TABLE_NAME));
            var date = BenchConst.initBatchDate();

            var threadList = new ArrayList<Thread>();
            var alive = new AtomicBoolean(true);
            for (int i = 0; i < 60; i++) {
                int factoryId = i + 1;
                var thread = new Thread(() -> {
                    dbManager.execute(setting, () -> {
                        debugManagerInTransaction(dbManager, factoryId, date);
                    });
                    while (alive.get()) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                threadList.add(thread);
                thread.start();
            }

            alive.set(false);

            LOG.info("thread join start");
            for (var thread : threadList) {
                try {
                    thread.join();
                } catch (Exception e) {
                    LOG.warn("join error", e);
                }
            }
            LOG.info("thread join end");
        }
    }

    private void debugManagerInTransaction(CostBenchDbManager dbManager, int factoryId, LocalDate date) {
        var dao = dbManager.getResultTableDao();
        LOG.info("factory{} start", factoryId);
        dao.deleteByFactory(factoryId, date);
        for (int i = 0; i < 1000; i++) {
            var entity = new ResultTable();
            entity.setRFId(factoryId);
            entity.setRManufacturingDate(date);
            entity.setRProductIId(100);
            entity.setRParentIId(1);
            entity.setRIId(i + 1);
            dao.insert(entity);
        }
        LOG.info("factory{} end", factoryId);
    }

    private void sessionLimit() throws IOException, InterruptedException {
        var endpoint = BenchConst.tsurugiEndpoint();
        LOG.info("endpoint={}", endpoint);
        var connector = TsurugiConnector.of(endpoint);

        var sessionList = new ArrayList<TsurugiSession>();
        try {
            for (int i = 1;; i++) {
                LOG.info("create session {}", i);
                var session = connector.createSession();
                sessionList.add(session);

                session.getLowSqlClient();
            }
        } finally {
            for (var session : sessionList) {
                session.close();
            }
        }
    }

    private void selectCost(String txType) {
        try (var dbManager = createDbManager()) {
            LOG.info("selectCost start");
            var setting = TgTmSetting.of(getTxOption(txType, CostMasterDao.TABLE_NAME));
            long start = System.currentTimeMillis();
            dbManager.execute(setting, () -> {
                var costMasterDao = dbManager.getCostMasterDao();
                int[] count = { 0 };
                try (var stream = costMasterDao.selectAll()) {
                    stream.forEach(cost -> count[0]++);
                }
                LOG.info("selectCost count={}", count[0]);
            });
            long end = System.currentTimeMillis();
            LOG.info("selectCost end {}", end - start);
        }
    }

    private TgTxOption getTxOption(String type, String wp) {
        switch (type.toUpperCase()) {
        case "OCC":
            return TgTxOption.ofOCC();
        case "LTX":
            return TgTxOption.ofLTX(wp);
        case "RTX":
            return TgTxOption.ofRTX();
        default:
            throw new IllegalArgumentException(type);
        }
    }

    private void insertStock(String txType, int recordSize) {
        try (var dbManager = createDbManager()) {
            LOG.info("insertStock start");
            var setting = TgTmSetting.of(getTxOption(txType, StockHistoryDao.TABLE_NAME));
            var date = BenchConst.initBatchDate();
            var time = LocalTime.now();
            var dao = dbManager.getStockHistoryDao();
            long start = System.currentTimeMillis();
            dbManager.execute(setting, () -> {
                for (int i = 0; i < recordSize; i++) {
                    var entity = new StockHistory();
                    entity.setSDate(date);
                    entity.setSFId((i % 60) + 1);
                    entity.setSIId(i);
                    entity.setSTime(time);
                    entity.setSStockUnit("test");
                    entity.setSStockQuantity(BigDecimal.ZERO);
                    entity.setSStockAmount(BigDecimal.ZERO);
                    dao.insert(entity);

                    if ((i + 1) % 10000 == 0) {
                        LOG.info("inserted {}", i + 1);
                    }
                }
            });
            long end = System.currentTimeMillis();
            LOG.info("insertStock end {}", end - start);
        }
    }

    private void copyStock(String txType) {
        try (var dbManager = createDbManager()) {
            LOG.info("copyStock start");
            var setting = TgTmSetting.of(getTxOption(txType, StockHistoryDao.TABLE_NAME));
            var date = BenchConst.initBatchDate();
            var time = LocalTime.now();
            var dao = dbManager.getStockHistoryDao();
            long start = System.currentTimeMillis();
            dbManager.execute(setting, () -> {
                var costMasterDao = dbManager.getCostMasterDao();
                int[] count = { 0 };
                try (var stream = costMasterDao.selectAll()) {
                    stream.forEach(cost -> {
                        int i = count[0];
                        var entity = new StockHistory();
                        entity.setSDate(date);
                        entity.setSFId((i % 60) + 1);
                        entity.setSIId(i);
                        entity.setSTime(time);
                        entity.setSStockUnit("test");
                        entity.setSStockQuantity(BigDecimal.ZERO);
                        entity.setSStockAmount(BigDecimal.ZERO);
                        dao.insert(entity);

                        if ((i + 1) % 10000 == 0) {
                            LOG.info("inserted {}", i + 1);
                        }
                        count[0]++;
                    });
                    LOG.info("copied {}", count[0]);
                }
            });
            long end = System.currentTimeMillis();
            LOG.info("copyStock end {}", end - start);
        }
    }

    private void copyStockList(String txType) {
        try (var dbManager = createDbManager()) {
            LOG.info("copyStock.list start");
            var setting = TgTmSetting.of(getTxOption(txType, StockHistoryDao.TABLE_NAME));
            var date = BenchConst.initBatchDate();
            var time = LocalTime.now();
            var dao = dbManager.getStockHistoryDao();
            long start = System.currentTimeMillis();
            dbManager.execute(setting, () -> {
                var costMasterDao = dbManager.getCostMasterDao();
                List<CostMaster> costList;
                try (var stream = costMasterDao.selectAll()) {
                    costList = stream.collect(Collectors.toList());
                }
                LOG.info("select {}", costList.size());
                int i = 0;
                for (@SuppressWarnings("unused")
                var cost : costList) {
                    var entity = new StockHistory();
                    entity.setSDate(date);
                    entity.setSFId((i % 60) + 1);
                    entity.setSIId(i);
                    entity.setSTime(time);
                    entity.setSStockUnit("test");
                    entity.setSStockQuantity(BigDecimal.ZERO);
                    entity.setSStockAmount(BigDecimal.ZERO);
                    dao.insert(entity);

                    if ((i + 1) % 10000 == 0) {
                        LOG.info("inserted {}", i + 1);
                    }
                    i++;
                }
                LOG.info("copied {}", i);
            });
            long end = System.currentTimeMillis();
            LOG.info("copyStock.list end {}", end - start);
        }
    }

    private CostBenchDbManager createDbManager() {
        var type = BenchConst.batchDbManagerType();
        var isolationLevel = BenchConst.batchJdbcIsolationLevel();
        boolean isMultiSession = BenchConst.batchExecuteType().equals(BenchConst.PARALLEL_FACTORY_SESSION);
        LOG.info("isMultiSession={}", isMultiSession);
        return CostBenchDbManager.createInstance(type, DbManagerPurpose.DEBUG, isolationLevel, isMultiSession);
    }
}
