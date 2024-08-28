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
package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.MeasurementMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;

public class MeasurementMasterDaoIceaxe extends IceaxeDao<MeasurementMaster> implements MeasurementMasterDao {

    private static final List<IceaxeColumn<MeasurementMaster, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<MeasurementMaster, ?>> list = new ArrayList<>();
        add(list, BenchVariable.ofString("m_unit"), MeasurementMaster::setMUnit, MeasurementMaster::getMUnit, IceaxeRecordUtil::getString, true);
        add(list, BenchVariable.ofString("m_name"), MeasurementMaster::setMName, MeasurementMaster::getMName, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofMeasurementType("m_type"), MeasurementMaster::setMType, MeasurementMaster::getMType, IceaxeRecordUtil::getMeasurementType);
        add(list, BenchVariable.ofString("m_default_unit"), MeasurementMaster::setMDefaultUnit, MeasurementMaster::getMDefaultUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("m_scale", MeasurementMaster.M_SCALE_SCALE), MeasurementMaster::setMScale, MeasurementMaster::getMScale, IceaxeRecordUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public MeasurementMasterDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, MeasurementMaster::new);
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
        return doInsert(entity, false);
    }

    @Override
    public List<MeasurementMaster> selectAll() {
        return doSelectAll();
    }

    @Override
    public void forEach(Consumer<MeasurementMaster> entityConsumer) {
        doForEach(entityConsumer);
    }
}
