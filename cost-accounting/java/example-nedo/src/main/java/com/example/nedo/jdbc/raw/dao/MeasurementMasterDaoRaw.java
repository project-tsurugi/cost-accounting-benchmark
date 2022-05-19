package com.example.nedo.jdbc.raw.dao;

import java.util.ArrayList;
import java.util.List;

import com.example.nedo.jdbc.doma2.dao.MeasurementMasterDao;
import com.example.nedo.jdbc.doma2.entity.MeasurementMaster;
import com.example.nedo.jdbc.raw.CostBenchDbManagerJdbc;

public class MeasurementMasterDaoRaw extends RawJdbcDao<MeasurementMaster> implements MeasurementMasterDao {

    private static final List<RawJdbcColumn<MeasurementMaster, ?>> COLUMN_LIST;
    static {
        List<RawJdbcColumn<MeasurementMaster, ?>> list = new ArrayList<>();
        add(list, "m_unit", MeasurementMaster::setMUnit, MeasurementMaster::getMUnit, RawJdbcUtil::setString, RawJdbcUtil::getString, true);
        add(list, "m_name", MeasurementMaster::setMName, MeasurementMaster::getMName, RawJdbcUtil::setString, RawJdbcUtil::getString);
        add(list, "m_type", MeasurementMaster::setMType, MeasurementMaster::getMType, RawJdbcUtil::setMeasurementType, RawJdbcUtil::getMeasurementType);
        add(list, "m_default_unit", MeasurementMaster::setMDefaultUnit, MeasurementMaster::getMDefaultUnit, RawJdbcUtil::setString, RawJdbcUtil::getString);
        add(list, "m_scale", MeasurementMaster::setMScale, MeasurementMaster::getMScale, RawJdbcUtil::setDecimal, RawJdbcUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public MeasurementMasterDaoRaw(CostBenchDbManagerJdbc dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST);
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
}
