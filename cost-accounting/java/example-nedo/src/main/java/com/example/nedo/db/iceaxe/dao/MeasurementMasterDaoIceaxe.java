package com.example.nedo.db.iceaxe.dao;

import java.util.ArrayList;
import java.util.List;

import com.example.nedo.db.doma2.dao.MeasurementMasterDao;
import com.example.nedo.db.doma2.entity.MeasurementMaster;
import com.example.nedo.db.iceaxe.CostBenchDbManagerIxeaxe;
import com.example.nedo.db.iceaxe.domain.TgVariableMeasurementType;
import com.tsurugidb.iceaxe.statement.TgVariable;

public class MeasurementMasterDaoIceaxe extends IceaxeDao<MeasurementMaster> implements MeasurementMasterDao {

    private static final List<IceaxeColumn<MeasurementMaster, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<MeasurementMaster, ?>> list = new ArrayList<>();
        add(list, TgVariable.ofCharacter("m_unit"), MeasurementMaster::setMUnit, MeasurementMaster::getMUnit, IceaxeRecordUtil::getString, true);
        add(list, TgVariable.ofCharacter("m_name"), MeasurementMaster::setMName, MeasurementMaster::getMName, IceaxeRecordUtil::getString);
        add(list, TgVariableMeasurementType.of("m_type"), MeasurementMaster::setMType, MeasurementMaster::getMType, IceaxeRecordUtil::getMeasurementType);
        add(list, TgVariable.ofCharacter("m_default_unit"), MeasurementMaster::setMDefaultUnit, MeasurementMaster::getMDefaultUnit, IceaxeRecordUtil::getString);
        add(list, TgVariable.ofDecimal("m_scale"), MeasurementMaster::setMScale, MeasurementMaster::getMScale, IceaxeRecordUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public MeasurementMasterDaoIceaxe(CostBenchDbManagerIxeaxe dbManager) {
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
