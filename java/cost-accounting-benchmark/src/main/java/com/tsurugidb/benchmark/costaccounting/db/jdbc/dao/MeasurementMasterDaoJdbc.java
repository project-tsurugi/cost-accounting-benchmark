package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

import java.util.ArrayList;
import java.util.List;

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
