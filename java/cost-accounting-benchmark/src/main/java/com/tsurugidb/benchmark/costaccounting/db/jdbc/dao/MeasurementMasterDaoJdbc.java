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
package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.MeasurementMaster;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;

public class MeasurementMasterDaoJdbc extends JdbcDao<MeasurementMaster> implements MeasurementMasterDao {

    private static final List<JdbcColumn<MeasurementMaster, ?>> COLUMN_LIST;
    static {
        List<JdbcColumn<MeasurementMaster, ?>> list = new ArrayList<>();
        add(list, "m_unit", MeasurementMaster::setMUnit, MeasurementMaster::getMUnit, JdbcUtil::setString, JdbcUtil::getString, true);
        add(list, "m_name", MeasurementMaster::setMName, MeasurementMaster::getMName, JdbcUtil::setString, JdbcUtil::getString);
        add(list, "m_type", MeasurementMaster::setMType, MeasurementMaster::getMType, JdbcUtil::setMeasurementType, JdbcUtil::getMeasurementType);
        add(list, "m_default_unit", MeasurementMaster::setMDefaultUnit, MeasurementMaster::getMDefaultUnit, JdbcUtil::setString, JdbcUtil::getString);
        add(list, "m_scale", MeasurementMaster::setMScale, MeasurementMaster::getMScale, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public MeasurementMasterDaoJdbc(CostBenchDbManagerJdbc dbManager) {
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
    public int insert(MeasurementMaster entity) {
        return doInsert(entity);
    }

    @Override
    public List<MeasurementMaster> selectAll() {
        return doSelectAll(MeasurementMaster::new);
    }

    @Override
    public void forEach(Consumer<MeasurementMaster> entityConsumer) {
        doForEach(MeasurementMaster::new, entityConsumer);
    }
}
