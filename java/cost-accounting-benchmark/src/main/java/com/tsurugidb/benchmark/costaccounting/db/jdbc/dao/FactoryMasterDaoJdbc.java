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
package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.getInt;
import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.setInt;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.FactoryMaster;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;

public class FactoryMasterDaoJdbc extends JdbcDao<FactoryMaster> implements FactoryMasterDao {

    private static final List<JdbcColumn<FactoryMaster, ?>> COLUMN_LIST;
    static {
        List<JdbcColumn<FactoryMaster, ?>> list = new ArrayList<>();
        add(list, "f_id", FactoryMaster::setFId, FactoryMaster::getFId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "f_name", FactoryMaster::setFName, FactoryMaster::getFName, JdbcUtil::setString, JdbcUtil::getString);
        COLUMN_LIST = list;
    }

    public FactoryMasterDaoJdbc(CostBenchDbManagerJdbc dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST);
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
        return doInsert(entity);
    }

    @Override
    public List<Integer> selectAllId() {
        String sql = "select f_id from " + TABLE_NAME;
        return executeQueryList(sql, null, rs -> getInt(rs, "f_id"));
    }

    @Override
    public FactoryMaster selectById(int factoryId) {
        String sql = "select * from " + TABLE_NAME + " where f_id = ?";
        return executeQuery1(sql, ps -> {
            int i = 1;
            setInt(ps, i++, factoryId);
        }, this::newEntity);
    }

    private FactoryMaster newEntity(ResultSet rs) throws SQLException {
        FactoryMaster entity = new FactoryMaster();
        fillEntity(entity, rs);
        return entity;
    }

    @Override
    public void forEach(Consumer<FactoryMaster> entityConsumer) {
        doForEach(FactoryMaster::new, entityConsumer);
    }
}
