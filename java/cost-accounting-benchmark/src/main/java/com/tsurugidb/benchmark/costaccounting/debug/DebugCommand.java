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
package com.tsurugidb.benchmark.costaccounting.debug;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager.DbManagerPurpose;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.debug.iceaxe_dbtest.DebugIceaxeWorkaround;
import com.tsurugidb.benchmark.costaccounting.debug.iceaxe_dbtest.DebugInsertDuplicate;
import com.tsurugidb.benchmark.costaccounting.debug.iceaxe_dbtest.DebugSelectFetch;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.util.FutureResponse;

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
        case "tsubakuro-rs.close":
            tsubakuroResultSetClose();
            break;
        case "tsubakuro-tx.close":
            tsubakuroTransactionClose();
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
        case "workaround":
            new DebugIceaxeWorkaround(args).execute();
            break;
        case "select-result":
            selectResult();
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

    private void tsubakuroResultSetClose() throws Exception {
        var endpoint = BenchConst.tsurugiEndpoint();
        LOG.info("endpoint={}", endpoint);
        var connector = Connector.create(endpoint);

        var sessionList = new ArrayList<Session>();
        var threadList = new ArrayList<Thread>();
        ResultSet[] resultSet = { null };
        {
            LOG.info("connect start");
            Session session;
            try {
                session = SessionBuilder.connect(connector).create(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                LOG.warn("connect error. {}: {}", e.getClass().getName(), e.getMessage());
                throw e;
            }
            LOG.info("session created. {}", session);
            sessionList.add(session);
            var sqlClient = SqlClient.attach(session);

            var thread = new Thread(() -> {
                var option = TransactionOption.newBuilder().setType(TransactionType.SHORT).build();
                try (var transaction = sqlClient.createTransaction(option).await()) {
                    try (var rs = transaction.executeQuery("select * from result_table").await()) {
                        resultSet[0] = rs;
                        while (rs.nextRow()) {
                        }
                    }
                } catch (ServerException | IOException | InterruptedException e) {
                    var message = e.getMessage();
                    LOG.info("{}.message={}", e.getClass().getSimpleName(), message);
                    if (message.contains("already closed")) {
                        return;
                    }
                    LOG.error("thread error", e);
                    throw new RuntimeException(e);
                } catch (Throwable e) {
                    LOG.error("thread error", e);
                    throw e;
                }
            });
            threadList.add(thread);
            thread.start();
        }

        while (resultSet[0] == null) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                LOG.warn("resultSet wait error", e);
            }
        }

        LOG.info("resultSet close start");
        try {
            resultSet[0].close();
        } catch (ServerException | IOException | InterruptedException e) {
            LOG.warn("resultSet close error", e);
        }
        LOG.info("resultSet close end");

        LOG.info("close session start");
        for (var session : sessionList) {
            try {
                session.close();
            } catch (Exception e) {
                LOG.warn("close error", e);
            }
        }
        LOG.info("close session end");

        LOG.info("thread join start");
        for (var thread : threadList) {
            try {
                thread.join();
            } catch (Exception e) {
                LOG.warn("thread join error", e);
            }
        }
        LOG.info("thread join end");
    }

    private void tsubakuroTransactionClose() throws Exception {
        var endpoint = BenchConst.tsurugiEndpoint();
        LOG.info("endpoint={}", endpoint);
        var connector = Connector.create(endpoint);

        var sessionList = new ArrayList<Session>();
        var threadList = new ArrayList<Thread>();
        try {
            final AtomicReference<FutureResponse<?>> transactionReference = new AtomicReference<>();
            {
                LOG.info("connect start");
                Session session;
                try {
                    session = SessionBuilder.connect(connector).create(1, TimeUnit.MINUTES);
                } catch (Exception e) {
                    LOG.warn("connect error. {}: {}", e.getClass().getName(), e.getMessage());
                    throw e;
                }
                LOG.info("session created. {}", session);
                sessionList.add(session);
                var sqlClient = SqlClient.attach(session);

                var thread = new Thread(() -> {
                    var option = TransactionOption.newBuilder().setType(TransactionType.SHORT).build();
                    LOG.info("transaction start");
                    try (var transaction0 = sqlClient.createTransaction(option)) {
                        LOG.info("transaction started. {}", transaction0);
                        transactionReference.set(transaction0);
                        LOG.info("transaction await start");
                        var transaction = transaction0.await();
                        LOG.info("transaction await end. {}", transaction);
                        try (var rs = transaction.executeQuery("select * from result_table").await()) {
                            while (rs.nextRow()) {
                            }
                        }
                    } catch (IOException e) {
                        var message = e.getMessage();
                        LOG.info("IOException.message={}", message);
                        if (message.contains("The wire was closed before receiving a response to this request")) {
                            return;
                        }
                        if (message.contains("the current session is already closed")) {
                            return;
                        }
                        if (message.contains("transaction already closed")) {
                            return;
                        }
                        if (message.contains("already closed")) { // wire
                            return;
                        }
                        if (message.contains("lost connection")) {
                            return;
                        }
                        LOG.error("thread error", e);
                        throw new RuntimeException(e);
                    } catch (ServerException e) {
                        var message = e.getMessage();
                        LOG.info("ServerException.message={}", message);
                        if (message.contains("Current transaction is inactive (maybe aborted already.)")) {
                            return;
                        }
                        if (message.contains("invalid tx handle")) {
                            return;
                        }
                        LOG.error("thread error", e);
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        LOG.error("thread error", e);
                        throw new RuntimeException(e);
                    } catch (Throwable e) {
                        LOG.error("thread error", e);
                        throw e;
                    }
                });
                threadList.add(thread);
                thread.start();
            }

            LOG.info("transactionFuture wait start");
            while (transactionReference.get() == null) {
            }
            LOG.info("transactionFuture wait end");

            LOG.info("transactionFuture close start");
            try {
                transactionReference.get().close();
            } catch (ServerException | IOException | InterruptedException e) {
                LOG.warn("transactionFuture close error", e);
            }
            LOG.info("transactionFuture close end");

        } finally {
            LOG.info("close session start");
            for (var session : sessionList) {
                try {
                    session.close();
                } catch (Exception e) {
                    LOG.warn("close error", e);
                }
            }
            LOG.info("close session end");

            LOG.info("thread join start");
            for (var thread : threadList) {
                try {
                    thread.join();
                } catch (Exception e) {
                    LOG.warn("thread join error", e);
                }
            }
            LOG.info("thread join end");
        }
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

    private void selectResult() throws InterruptedException {
        Thread thread;
        try (var dbManager = createDbManager()) {
            thread = new Thread(() -> {
                LOG.info("selectResult start");
                var setting = TgTmSetting.of(TgTxOption.ofOCC());
                long start = System.currentTimeMillis();
                dbManager.execute(setting, () -> {
                    var dao = dbManager.getResultTableDao();
                    int[] count = { 0 };
                    dao.forEach(entity -> {
                        count[0]++;
                    });
                    LOG.info("selectResult count={}", count[0]);
                });
                long end = System.currentTimeMillis();
                LOG.info("selectResult end {}", end - start);
            });
            thread.start();
            Thread.sleep(2000);
        }

        LOG.info("thread join start");
        thread.join();
        LOG.info("thread join end");
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
