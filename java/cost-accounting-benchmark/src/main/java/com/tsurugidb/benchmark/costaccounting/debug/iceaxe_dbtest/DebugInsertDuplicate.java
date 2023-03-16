package com.tsurugidb.benchmark.costaccounting.debug.iceaxe_dbtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableInteger;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableString;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * iceaxe-dbtest DbInsertDuplicate2Test
 */
public class DebugInsertDuplicate {
    private static final Logger LOG = LoggerFactory.getLogger(DebugInsertDuplicate.class);

    private TgTxOption txOption;
    private int onlineSize;
    private int attemptSize;

    public DebugInsertDuplicate(String[] args) {
        try {
            switch (args[2].toUpperCase()) {
            case "OCC":
                this.txOption = TgTxOption.ofOCC();
                break;
            case "LTX":
            default:
                this.txOption = TgTxOption.ofLTX(TEST, TEST2);
                break;
            }
            if (args.length > 3) {
                this.onlineSize = Integer.parseInt(args[3]);
            } else {
                this.onlineSize = 60;
            }
            if (args.length > 4) {
                this.attemptSize = Integer.parseInt(args[4]);
            } else {
                this.attemptSize = 15000;
            }
        } catch (Exception e) {
            System.err.println("args: {OCC|LTX} [threadSize] [attemptSize]");
            throw e;
        }
    }

    public void execute() throws Exception {
        var endpoint = BenchConst.tsurugiEndpoint();
        LOG.info("endpoint={}", endpoint);
        var connector = TsurugiConnector.of(endpoint);

        initializeTable(connector);
        executeMain(new DbSessions(connector));

        LOG.info("end");
    }

    private static final String TEST = "test";
    private static final String TEST2 = "test2";

    private static final String CREATE_TEST_SQL = "create table " + TEST //
            + "(" //
            + "  foo int," //
            + "  bar bigint," //
            + "  zzz varchar(10)," //
            + "  primary key(foo)" //
            + ")";
    private static final String CREATE_TEST2_SQL = "create table " + TEST2 //
            + "(" //
            + "  key1 int," //
            + "  key2 int," //
            + "  zzz2 varchar(10)," //
            + "  primary key(key1, key2)" //
            + ")";

    private void initializeTable(TsurugiConnector connector) throws IOException {
        try (var session = connector.createSession()) {
            createTable(session, TEST, CREATE_TEST_SQL);
            createTable(session, TEST2, CREATE_TEST2_SQL);
            insertTestTable(session, 10);
        }
    }

    private static void createTable(TsurugiSession session, String tableName, String createSql) throws IOException {
        var tm = session.createTransactionManager();
        if (session.findTableMetadata(tableName).isPresent()) {
            tm.executeDdl("drop table " + tableName);
        }

        tm.executeDdl(createSql);
    }

    protected static final String INSERT_SQL = "insert into " + TEST //
            + "(foo, bar, zzz)" //
            + "values(:foo, :bar, :zzz)";
    protected static final TgEntityParameterMapping<TestEntity> INSERT_MAPPING = TgParameterMapping.of(TestEntity.class) //
            .addInt("foo", TestEntity::getFoo) //
            .addLong("bar", TestEntity::getBar) //
            .addString("zzz", TestEntity::getZzz);

    protected static void insertTestTable(TsurugiSession session, int size) throws IOException {
        var tm = session.createTransactionManager(TgTxOption.ofLTX(TEST));
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                for (int i = 0; i < size; i++) {
                    var entity = createTestEntity(i);
                    transaction.executeAndGetCount(ps, entity);
                }
            });
        }
    }

    protected static TestEntity createTestEntity(int i) {
        return new TestEntity(i, i, Integer.toString(i));
    }

    private void executeMain(DbSessions sessions) throws Exception {
        LOG.info("txOption={}", txOption);
        LOG.info("threadSize={}", onlineSize);
        LOG.info("attemptSize={}", attemptSize);

        try (sessions) {
            var onlineList = new ArrayList<OnlineTask>(onlineSize);
            for (int i = 0; i < onlineSize; i++) {
                var task = new OnlineTask(sessions.createSession(), txOption, attemptSize);
                onlineList.add(task);
            }

            var service = Executors.newCachedThreadPool();
            var futureList = new ArrayList<Future<?>>(onlineSize);
            onlineList.forEach(task -> futureList.add(service.submit(task)));

            RuntimeException save = null;
            for (var future : futureList) {
                try {
                    future.get();
                } catch (Exception e) {
                    if (save == null) {
                        save = new RuntimeException("future exception");
                    }
                    save.addSuppressed(e);
                }
            }
            if (save != null) {
                throw save;
            }
        }
    }

    private static class OnlineTask implements Callable<Void> {
        private static final Logger LOG = LoggerFactory.getLogger(OnlineTask.class);

        private static final AtomicInteger INSERT_COUNT = new AtomicInteger(0);

        private static final TgBindVariableInteger vKey1 = TgBindVariable.ofInt("key1");
        private static final TgBindVariableInteger vKey2 = TgBindVariable.ofInt("key2");
        private static final TgBindVariableString vZzz2 = TgBindVariable.ofString("zzz2");

        private final TsurugiSession session;
        private final TgTxOption txOption;
        private final int attemptSize;

        public OnlineTask(TsurugiSession session, TgTxOption txOption, int attemptSize) {
            this.session = session;
            this.txOption = txOption;
            this.attemptSize = attemptSize;
        }

        @Override
        public Void call() throws Exception {
            var maxSql = "select max(foo) + 1 as foo from " + TEST;

            var insert2List = List.of(vKey1, vKey2, vZzz2);
            var insert2Sql = "insert into " + TEST2 //
                    + "(key1, key2, zzz2)" //
                    + "values(" + insert2List.stream().map(v -> v.sqlName()).collect(Collectors.joining(", ")) + ")";
            var insert2Mapping = TgParameterMapping.of(insert2List);

            try (var maxPs = session.createQuery(maxSql); //
                    var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                    var insert2Ps = session.createStatement(insert2Sql, insert2Mapping)) {
                var setting = TgTmSetting.ofAlways(txOption);
                var tm = session.createTransactionManager(setting);

                for (;;) {
                    if (INSERT_COUNT.get() >= attemptSize) {
                        break;
                    }
                    try {
                        tm.execute(transaction -> {
                            execute(transaction, maxPs, insertPs, insert2Ps);
                        });
                    } catch (TsurugiTransactionIOException e) {
                        if (e.getDiagnosticCode() == SqlServiceCode.ERR_ALREADY_EXISTS) {
//                          LOG.info("ERR_ALREADY_EXISTS {}", i);
                            continue;
                        }
                        LOG.error("online task error: {}", e.getMessage());
                        throw e;
                    }
                    INSERT_COUNT.incrementAndGet();
                }
            }
            return null;
        }

        private void execute(TsurugiTransaction transaction, TsurugiSqlQuery<TsurugiResultEntity> maxPs, TsurugiSqlPreparedStatement<TestEntity> insertPs,
                TsurugiSqlPreparedStatement<TgBindParameters> insert2Ps) throws IOException, TsurugiTransactionException {
            var max = transaction.executeAndFindRecord(maxPs).get();
            int foo = max.getInt("foo");

            var entity = new TestEntity(foo, foo, Integer.toString(foo));
            transaction.executeAndGetCount(insertPs, entity);

            for (int i = 0; i < 10; i++) {
                var parameter = TgBindParameters.of(vKey1.bind(foo), vKey2.bind(i + 1), vZzz2.bind(Integer.toString(foo)));
                transaction.executeAndGetCount(insert2Ps, parameter);
            }
        }
    }
}
