/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.benchmark.costaccounting.debug.iceaxe_dbtest;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;

/**
 * iceaxe-dbtest DbSelectFetchTest
 */
public class DebugSelectFetch extends DbTester {
    private static final Logger LOG = LoggerFactory.getLogger(DebugSelectFetch.class);

    private int size;

    public DebugSelectFetch(String[] args) {
        try {
            this.size = Integer.parseInt(args[2]);
        } catch (Exception e) {
            System.err.println("args: {record count}");
            throw e;
        }
    }

    public void execute() throws IOException, InterruptedException {
        try {
            beforeAll();
            fetch();
            LOG.info("end");
        } finally {
            closeSession();
        }
    }

    private void beforeAll() throws IOException, InterruptedException {
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(size);

        dropTable(TEST2);
        createTest2Table();

        LOG.debug("init end");
    }

    private static final String TEST2 = "test2";

    private void createTest2Table() throws IOException, InterruptedException {
        var sql = CREATE_TEST_SQL.replace(TEST, TEST2);
        executeDdl(getSession(), sql);
    }

    void fetch() throws IOException, InterruptedException {
        LOG.info("size={}", size);

        var cond = TgBindVariable.ofInt("foo");
        var sql = SELECT_SQL + " where foo=" + cond;
        var parameterMapping = TgParameterMapping.of(cond);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery("select foo from " + TEST); //
                var ps2 = session.createQuery(sql, parameterMapping, SELECT_MAPPING); //
                var ps3 = session.createStatement(INSERT_SQL.replace(TEST, TEST2), INSERT_MAPPING)) {
            tm.execute(transaction -> {
                int[] count = { 0 };
                transaction.executeAndForEach(ps, fetch -> {
                    int foo = fetch.getInt("foo");
                    var parameter = TgBindParameters.of(cond.bind(foo));
                    var entity = transaction.executeAndFindRecord(ps2, parameter).get();
                    transaction.executeAndGetCount(ps3, entity);
                    if (++count[0] % 1000 == 0) {
                        LOG.info("processed={}", count[0]);
                    }
                });
            });
        }
    }
}
