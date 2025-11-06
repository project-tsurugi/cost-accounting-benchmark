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

import com.tsurugidb.benchmark.costaccounting.db.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.MeasurementMaster;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

public class MeasurementMasterDaoTsubakuro extends TsubakuroDao<MeasurementMaster> implements MeasurementMasterDao {

    private static final List<TsubakuroColumn<MeasurementMaster, ?>> COLUMN_LIST;
    static {
        List<TsubakuroColumn<MeasurementMaster, ?>> list = new ArrayList<>();
        add(list, "m_unit", AtomType.CHARACTER, MeasurementMaster::setMUnit, MeasurementMaster::getMUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString, true);
        add(list, "m_name", AtomType.CHARACTER, MeasurementMaster::setMName, MeasurementMaster::getMName, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "m_type", AtomType.CHARACTER, MeasurementMaster::setMType, MeasurementMaster::getMType, TsubakuroUtil::getParameter, TsubakuroUtil::getMeasurementType);
        add(list, "m_default_unit", AtomType.CHARACTER, MeasurementMaster::setMDefaultUnit, MeasurementMaster::getMDefaultUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "m_scale", AtomType.DECIMAL, MeasurementMaster::setMScale, MeasurementMaster::getMScale, (name, value) -> TsubakuroUtil.getParameter(name, value, MeasurementMaster.M_SCALE_SCALE),
                TsubakuroUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public MeasurementMasterDaoTsubakuro(CostBenchDbManagerTsubakuro dbManager) {
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
        throw new UnsupportedOperationException("not yet impl");
    }
}
