package com.tsurugidb.benchmark.costaccounting.debug.iceaxe_dbtest;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.result.TgEntityResultMapping;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class DbTester {
    private static final Logger LOG = LoggerFactory.getLogger(DbTester.class);

    /** test (table name) */
    public static final String TEST = "test";
    public static final String TEST_COLUMNS = "foo, bar, zzz";
    public static final int ZZZ_SIZE = 10;

    private TsurugiSession staticSession;

    protected TsurugiSession getSession() throws IOException {
        if (staticSession == null) {
            var endpoint = BenchConst.tsurugiEndpoint();
            LOG.info("endpoint={}", endpoint);
            var connector = TsurugiConnector.createConnector(endpoint);

            var info = TgSessionInfo.of();
            staticSession = connector.createSession(info);
        }
        return staticSession;
    }

    protected void closeSession() throws IOException {
        if (staticSession != null) {
            staticSession.close();
        }
    }

    // utility

    protected void dropTestTable() throws IOException {
        dropTable(TEST);
    }

    protected void dropTable(String tableName) throws IOException {
        if (existsTable(tableName)) {
            var sql = "drop table " + tableName;
            executeDdl(getSession(), sql);
        }
    }

    protected boolean existsTable(String tableName) throws IOException {
        var session = getSession();
        var opt = session.findTableMetadata(tableName);
        return opt.isPresent();
    }

    protected static final String CREATE_TEST_SQL = "create table " + TEST //
            + "(" //
            + "  foo int," //
            + "  bar bigint," //
            + "  zzz varchar(" + ZZZ_SIZE + ")," //
            + "  primary key(foo)" //
            + ")";

    protected void createTestTable() throws IOException {
        executeDdl(getSession(), CREATE_TEST_SQL);
    }

    protected void executeDdl(TsurugiSession session, String sql) throws IOException {
        var tm = session.createTransactionManager();
        tm.executeDdl(sql);
    }

    protected static final String INSERT_SQL = "insert into " + TEST //
            + "(" + TEST_COLUMNS + ")" //
            + "values(:foo, :bar, :zzz)";
    protected static final TgEntityParameterMapping<TestEntity> INSERT_MAPPING = TgParameterMapping.of(TestEntity.class) //
            .int4("foo", TestEntity::getFoo) //
            .int8("bar", TestEntity::getBar) //
            .character("zzz", TestEntity::getZzz);

    protected void insertTestTable(int size) throws IOException {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofLTX(TEST));
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                for (int i = 0; i < size; i++) {
                    var entity = createTestEntity(i);
                    transaction.executeAndGetCount(ps, entity);
                    if ((i + 1) % 1000 == 0) {
                        LOG.info("inserted {}", i + 1);
                    }
                }
            });
        }
    }

    protected TestEntity createTestEntity(int i) {
        return new TestEntity(i, i, Integer.toString(i));
    }

    protected void insertTestTable(TestEntity entity) throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session, 3);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                transaction.executeAndGetCount(ps, entity);
            });
        }
    }

    protected static final String SELECT_SQL = "select " + TEST_COLUMNS + " from " + TEST;

    protected static final TgEntityResultMapping<TestEntity> SELECT_MAPPING = TgResultMapping.of(TestEntity::new) //
            .int4("foo", TestEntity::setFoo) //
            .int8("bar", TestEntity::setBar) //
            .character("zzz", TestEntity::setZzz);

    // transaction manager

    protected static TsurugiTransactionManager createTransactionManagerOcc(TsurugiSession session) {
        return session.createTransactionManager(TgTxOption.ofOCC());
    }

    protected static TsurugiTransactionManager createTransactionManagerOcc(TsurugiSession session, int max) {
        return session.createTransactionManager(TgTmSetting.ofAlways(TgTxOption.ofOCC(), 3));
    }

}
