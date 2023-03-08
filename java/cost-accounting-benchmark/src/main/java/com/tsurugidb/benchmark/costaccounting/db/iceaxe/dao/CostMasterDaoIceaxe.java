package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;

public class CostMasterDaoIceaxe extends IceaxeDao<CostMaster> implements CostMasterDao {

    private static final TgVariableInteger C_F_ID = BenchVariable.ofInt("c_f_id");
    private static final TgVariableInteger C_I_ID = BenchVariable.ofInt("c_i_id");
    private static final List<IceaxeColumn<CostMaster, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<CostMaster, ?>> list = new ArrayList<>();
        add(list, C_F_ID, CostMaster::setCFId, CostMaster::getCFId, IceaxeRecordUtil::getInt, true);
        add(list, C_I_ID, CostMaster::setCIId, CostMaster::getCIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofString("c_stock_unit"), CostMaster::setCStockUnit, CostMaster::getCStockUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("c_stock_quantity", CostMaster.C_STOCK_QUANTITY_SCALE), CostMaster::setCStockQuantity, CostMaster::getCStockQuantity, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("c_stock_amount", CostMaster.C_STOCK_AMOUNT_SCALE), CostMaster::setCStockAmount, CostMaster::getCStockAmount, IceaxeRecordUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public CostMasterDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, CostMaster::new);
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
    public int insert(CostMaster entity) {
        return doInsert(entity);
    }

    private static final TgVariableInteger vFactoryId = C_F_ID.copy("fId");

    @Override
    public List<CostMaster> selectByFactory(int fId) {
        var ps = selectByFactoryCache.get();
        var param = TgParameterList.of(vFactoryId.bind(fId));
        return executeAndGetList(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, CostMaster> selectByFactoryCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where c_f_id = " + vFactoryId;
            this.parameterMapping = TgParameterMapping.of(vFactoryId);
            this.resultMapping = getEntityResultMapping();
        }
    };

    private static final TgVariableInteger vItemId = C_I_ID.copy("iId");

    @Override
    public CostMaster selectById(int fId, int iId) {
        var ps = selectByIdCache.get();
        var param = TgParameterList.of(vFactoryId.bind(fId), vItemId.bind(iId));
        return executeAndGetRecord(ps, param);
    }

    private final CachePreparedQuery<TgParameterList, CostMaster> selectByIdCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where c_f_id = " + vFactoryId + " and c_i_id = " + vItemId;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vItemId);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public CostMaster lock(CostMaster in) {
        // Tsurugiにselect for updateは無い
        return in;
    }

    @Override
    public Stream<CostMaster> selectOrderIid() {
        var ps = selectOrderIidCache.get();
        return executeAndGetStream(ps);
    }

    private final CacheQuery<CostMaster> selectOrderIidCache = new CacheQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " order by c_i_id";
            this.resultMapping = getEntityResultMapping();
        }
    };

    private static final TgVariable<BigDecimal> vQuantity = BenchVariable.ofDecimal("quantity", CostMaster.C_STOCK_QUANTITY_SCALE);
    private static final TgVariable<BigDecimal> vAmount = BenchVariable.ofDecimal("amount", CostMaster.C_STOCK_AMOUNT_SCALE);

    @Override
    public int updateIncrease(CostMaster entity, BigDecimal quantity, BigDecimal amount) {
        var ps = updateIncreaseCache.get();
        var param = TgParameterList.of(vFactoryId.bind(entity.getCFId()), vItemId.bind(entity.getCIId()), vQuantity.bind(quantity), vAmount.bind(amount));
        return executeAndGetCount(ps, param);
    }

    private final CachePreparedStatement<TgParameterList> updateIncreaseCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            this.sql = "update " + TABLE_NAME + " set" //
                    + " c_stock_quantity = c_stock_quantity + " + vQuantity //
                    + ",c_stock_amount = c_stock_amount + " + vAmount //
                    + " where c_f_id=" + vFactoryId + " and c_i_id=" + vItemId;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vItemId, vQuantity, vAmount);
        }
    };

    @Override
    public int updateDecrease(CostMaster entity, BigDecimal quantity) {
        var ps = updateDecreaseCache.get();
        var param = TgParameterList.of(vFactoryId.bind(entity.getCFId()), vItemId.bind(entity.getCIId()), vQuantity.bind(quantity));
        return executeAndGetCount(ps, param);
    }

    private final CachePreparedStatement<TgParameterList> updateDecreaseCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            String stockAmountType = "decimal(15," + CostMaster.C_STOCK_AMOUNT_SCALE + ")";
            this.sql = "update " + TABLE_NAME + " set" //
                    + " c_stock_quantity = c_stock_quantity - " + vQuantity //
                    + ",c_stock_amount = cast(c_stock_amount - c_stock_amount * " + vQuantity + " / c_stock_quantity as " + stockAmountType + ")" //
                    + " where c_f_id=" + vFactoryId + " and c_i_id=" + vItemId;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vItemId, vQuantity);
        }
    };

    @Override
    public void forEach(Consumer<CostMaster> entityConsumer) {
        doForEach(entityConsumer);
    }
}
