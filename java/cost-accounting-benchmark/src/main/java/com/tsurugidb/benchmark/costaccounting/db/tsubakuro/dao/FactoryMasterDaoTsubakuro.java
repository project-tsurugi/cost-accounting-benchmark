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
package com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.FactoryMaster;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

public class FactoryMasterDaoTsubakuro extends TsubakuroDao<FactoryMaster> implements FactoryMasterDao {

    private static final List<TsubakuroColumn<FactoryMaster, ?>> COLUMN_LIST;
    static {
        List<TsubakuroColumn<FactoryMaster, ?>> list = new ArrayList<>();
        add(list, "f_id", AtomType.INT4, FactoryMaster::setFId, FactoryMaster::getFId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "f_name", AtomType.CHARACTER, FactoryMaster::setFName, FactoryMaster::getFName, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        COLUMN_LIST = list;
    }

    public FactoryMasterDaoTsubakuro(CostBenchDbManagerTsubakuro dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, FactoryMaster::new);
    }

    @Override
    public void truncate() {
        doTruncate();
    }

    @Override
    public int deleteAll() {
        return doDeleteAll();
    }

    @Override
    public int insert(FactoryMaster entity) {
        return doInsert(entity, false);
    }

    @Override
    public List<Integer> selectAllId() {
        var sql = "select f_id from " + TABLE_NAME;
        return executeAndGetList(sql, rs -> {
            if (rs.nextColumn()) {
                return rs.fetchInt4Value();
            }
            throw new AssertionError();
        });
    }

    @Override
    public FactoryMaster selectById(int factoryId) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public void forEach(Consumer<FactoryMaster> entityConsumer) {
        throw new UnsupportedOperationException("not yet impl");
    }
}
