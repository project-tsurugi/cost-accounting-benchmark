package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIxeaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;

public class CostMasterDaoIceaxe extends IceaxeDao<CostMaster> implements CostMasterDao {

    private static final TgVariableInteger C_F_ID = BenchVariable.ofInt("c_f_id");
    private static final TgVariableInteger C_I_ID = BenchVariable.ofInt("c_i_id");
    private static final List<IceaxeColumn<CostMaster, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<CostMaster, ?>> list = new ArrayList<>();
        add(list, C_F_ID, CostMaster::setCFId, CostMaster::getCFId, IceaxeRecordUtil::getInt, true);
        add(list, C_I_ID, CostMaster::setCIId, CostMaster::getCIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofString("c_stock_unit"), CostMaster::setCStockUnit, CostMaster::getCStockUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("c_stock_quantity"), CostMaster::setCStockQuantity, CostMaster::getCStockQuantity, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("c_stock_amount"), CostMaster::setCStockAmount, CostMaster::getCStockAmount, IceaxeRecordUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public CostMasterDaoIceaxe(CostBenchDbManagerIxeaxe dbManager) {
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

    private static final TgVariableInteger vFactoryId = C_F_ID.copy("fId");

    @Override
    public List<CostMaster> selectByFactory(int fId) {
        var ps = getSelectByFactoryPs();
        var param = TgParameterList.of(vFactoryId.bind(fId));
        return executeAndGetList(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, CostMaster> selectByFactoryPs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, CostMaster> getSelectByFactoryPs() {
        if (this.selectByFactoryPs == null) {
            var sql = getSelectEntitySql() + " where c_f_id = " + vFactoryId;
            var parameterMapping = TgParameterMapping.of(vFactoryId);
            var resultMapping = getEntityResultMapping();
            this.selectByFactoryPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectByFactoryPs;
    }

    private static final TgVariableInteger vItemId = C_I_ID.copy("iId");

    @Override
    public CostMaster selectById(int fId, int iId) {
        var ps = getSelectByIdPs();
        var param = TgParameterList.of(vFactoryId.bind(fId), vItemId.bind(iId));
        return executeAndGetRecord(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, CostMaster> selectByIdPs;

    private synchronized TsurugiPreparedStatementQuery1<TgParameterList, CostMaster> getSelectByIdPs() {
        if (this.selectByIdPs == null) {
            var sql = getSelectEntitySql() + " where c_f_id = " + vFactoryId + " and c_i_id = " + vItemId;
            var parameterMapping = TgParameterMapping.of(vFactoryId, vItemId);
            var resultMapping = getEntityResultMapping();
            this.selectByIdPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectByIdPs;
    }

    @Override
    public CostMaster lock(CostMaster in) {
        // Tsurugiにselect for updateは無い
        return in;
    }

    private static final TgVariable<BigDecimal> vQuantity = BenchVariable.ofDecimal("quantity");
    private static final TgVariable<BigDecimal> vAmount = BenchVariable.ofDecimal("amount");

    @Override
    public int updateIncrease(CostMaster entity, BigDecimal quantity, BigDecimal amount) {
        var ps = getUpdateIncreasePs();
        var param = TgParameterList.of(vFactoryId.bind(entity.getCFId()), vItemId.bind(entity.getCIId()), vQuantity.bind(quantity), vAmount.bind(amount));
        return executeAndGetCount(ps, param);
    }

    private TsurugiPreparedStatementUpdate1<TgParameterList> updateIncreasePs;

    private synchronized TsurugiPreparedStatementUpdate1<TgParameterList> getUpdateIncreasePs() {
        if (this.updateIncreasePs == null) {
            var sql = "update " + TABLE_NAME + " set" //
                    + " c_stock_quantity = c_stock_quantity + " + vQuantity //
                    + ",c_stock_amount = c_stock_amount + " + vAmount //
                    + " where c_f_id=" + vFactoryId + " and c_i_id=" + vItemId;
            var parameterMapping = TgParameterMapping.of(vFactoryId, vItemId, vQuantity, vAmount);
            this.updateIncreasePs = createPreparedStatement(sql, parameterMapping);
        }
        return this.updateIncreasePs;
    }

    @Override
    public int updateDecrease(CostMaster entity, BigDecimal quantity) {
        var ps = getUpdateDecreasePs();
        var param = TgParameterList.of(vFactoryId.bind(entity.getCFId()), vItemId.bind(entity.getCIId()), vQuantity.bind(quantity));
        return executeAndGetCount(ps, param);
    }

    private TsurugiPreparedStatementUpdate1<TgParameterList> updateDecreasePs;

    private synchronized TsurugiPreparedStatementUpdate1<TgParameterList> getUpdateDecreasePs() {
        if (this.updateDecreasePs == null) {
            var sql = "update " + TABLE_NAME + " set" //
                    + " c_stock_quantity = c_stock_quantity - " + vQuantity //
                    + ",c_stock_amount = c_stock_amount - c_stock_amount * " + vQuantity + " / c_stock_quantity" //
                    + " where c_f_id=" + vFactoryId + " and c_i_id=" + vItemId;
            var parameterMapping = TgParameterMapping.of(vFactoryId, vItemId, vQuantity);
            this.updateDecreasePs = createPreparedStatement(sql, parameterMapping);
        }
        return this.updateDecreasePs;
    }
}
