package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.util.ArrayList;
import java.util.List;

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
        add(list, BenchVariable.ofDecimal("m_scale"), MeasurementMaster::setMScale, MeasurementMaster::getMScale, IceaxeRecordUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public MeasurementMasterDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, MeasurementMaster::new);
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
        return doSelectAll();
    }
}
