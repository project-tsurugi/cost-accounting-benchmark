package com.tsurugidb.benchmark.costaccounting.debug;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.debug.iceaxe_dbtest.DebugInsertDuplicate;
import com.tsurugidb.benchmark.costaccounting.debug.iceaxe_dbtest.DebugSelectFetch;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
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
        case "1":
            debug(false, false);
            break;
        case "2":
            debug(true, false);
            break;
        case "3":
            debug(false, true);
            break;
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
        default:
            throw new UnsupportedOperationException("unsupported operation. type=" + type);
        }

        return 0;
    }

    private void debug(boolean sqlClient, boolean transaction) throws IOException {
        var endpoint = BenchConst.tsurugiEndpoint();
        LOG.info("endpoint={}", endpoint);
        var connector = TsurugiConnector.createConnector(endpoint);
        var info = TgSessionInfo.of();

        LOG.info("create session start");
        var list = new ArrayList<TsurugiSession>();
        for (int i = 0; i < 60; i++) {
            var session = connector.createSession(info);
            list.add(session);

            if (sqlClient) {
                session.getLowSqlClient();
            }
        }
        LOG.info("create session end");

        if (transaction) {
            LOG.info("createTransaction start");
            int i = 0;
            for (var session : list) {
                LOG.info("createTransaction {}", i++);
                try (var tx = session.createTransaction(TgTxOption.ofOCC())) {
                    tx.getLowTransaction();
                }
            }
            LOG.info("createTransaction end");
        }

        LOG.info("close session start");
        for (var session : list) {
            session.close();
        }
        LOG.info("close session end");
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

    private void sessionLimit() throws IOException {
        var endpoint = BenchConst.tsurugiEndpoint();
        LOG.info("endpoint={}", endpoint);
        var connector = TsurugiConnector.createConnector(endpoint);

        var sessionList = new ArrayList<TsurugiSession>();
        try {
            for (int i = 1;; i++) {
                LOG.info("create session {}", i);
                var info = TgSessionInfo.of();
                var session = connector.createSession(info);
                sessionList.add(session);

                session.getLowSqlClient();
            }
        } finally {
            for (var session : sessionList) {
                session.close();
            }
        }
    }

    private CostBenchDbManager createDbManager() {
        var type = BenchConst.batchDbManagerType();
        var isolationLevel = BenchConst.batchJdbcIsolationLevel();
        boolean isMultiSession = BenchConst.batchExecuteType().equals(BenchConst.PARALLEL_FACTORY_SESSION);
        LOG.info("isMultiSession={}", isMultiSession);
        return CostBenchDbManager.createInstance(type, isolationLevel, isMultiSession);
    }
}
