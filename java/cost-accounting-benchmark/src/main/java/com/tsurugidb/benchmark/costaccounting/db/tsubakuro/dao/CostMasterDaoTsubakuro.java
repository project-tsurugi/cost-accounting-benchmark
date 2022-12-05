package com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

public class CostMasterDaoTsubakuro extends TsubakuroDao<CostMaster> implements CostMasterDao {

    private static final List<TsubakuroColumn<CostMaster, ?>> COLUMN_LIST;
    static {
        List<TsubakuroColumn<CostMaster, ?>> list = new ArrayList<>();
        add(list, "c_f_id", AtomType.INT4, CostMaster::setCFId, CostMaster::getCFId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "c_i_id", AtomType.INT4, CostMaster::setCIId, CostMaster::getCIId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "c_stock_unit", AtomType.CHARACTER, CostMaster::setCStockUnit, CostMaster::getCStockUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "c_stock_quantity", AtomType.DECIMAL, CostMaster::setCStockQuantity, CostMaster::getCStockQuantity,
                (name, value) -> TsubakuroUtil.getParameter(name, value, CostMaster.C_STOCK_QUANTITY_SCALE), TsubakuroUtil::getDecimal);
        add(list, "c_stock_amount", AtomType.DECIMAL, CostMaster::setCStockAmount, CostMaster::getCStockAmount,
                (name, value) -> TsubakuroUtil.getParameter(name, value, CostMaster.C_STOCK_AMOUNT_SCALE), TsubakuroUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public CostMasterDaoTsubakuro(CostBenchDbManagerTsubakuro dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, CostMaster::new);
    }

    @Override
    public int deleteAll() {
        return doDeleteAll();
    }

    @Override
    public int insert(CostMaster entity) {
        return doInsert(entity);
    }

    @Override
    public List<CostMaster> selectByFactory(int fId) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public CostMaster selectById(int fId, int iId) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public CostMaster lock(CostMaster in) {
        // Tsurugiにselect for updateは無い
        return in;
    }

    @Override
    public int updateIncrease(CostMaster entity, BigDecimal quantity, BigDecimal amount) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public int updateDecrease(CostMaster entity, BigDecimal quantity) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public void forEach(Consumer<CostMaster> entityConsumer) {
        throw new UnsupportedOperationException("not yet impl");
    }
}
