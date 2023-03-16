package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableInteger;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;

public class ResultTableDaoIceaxe extends IceaxeDao<ResultTable> implements ResultTableDao {

    private static final TgBindVariableInteger R_F_ID = BenchVariable.ofInt("r_f_id");
    private static final TgBindVariableInteger R_PRODUCT_I_ID = BenchVariable.ofInt("r_product_i_id");
    private static final List<IceaxeColumn<ResultTable, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<ResultTable, ?>> list = new ArrayList<>();
        add(list, R_F_ID, ResultTable::setRFId, ResultTable::getRFId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofDate("r_manufacturing_date"), ResultTable::setRManufacturingDate, ResultTable::getRManufacturingDate, IceaxeRecordUtil::getDate, true);
        add(list, R_PRODUCT_I_ID, ResultTable::setRProductIId, ResultTable::getRProductIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofInt("r_parent_i_id"), ResultTable::setRParentIId, ResultTable::getRParentIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofInt("r_i_id"), ResultTable::setRIId, ResultTable::getRIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofBigInt("r_manufacturing_quantity"), ResultTable::setRManufacturingQuantity, ResultTable::getRManufacturingQuantity, IceaxeRecordUtil::getBigInt);
        add(list, BenchVariable.ofString("r_weight_unit"), ResultTable::setRWeightUnit, ResultTable::getRWeightUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("r_weight", ResultTable.R_WEIGHT_SCALE), ResultTable::setRWeight, ResultTable::getRWeight, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofString("r_weight_total_unit"), ResultTable::setRWeightTotalUnit, ResultTable::getRWeightTotalUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("r_weight_total", ResultTable.R_WEIGHT_TOTAL_SCALE), ResultTable::setRWeightTotal, ResultTable::getRWeightTotal, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("r_weight_ratio", ResultTable.R_WEIGHT_RATIO_SCALE), ResultTable::setRWeightRatio, ResultTable::getRWeightRatio, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofString("r_standard_quantity_unit"), ResultTable::setRStandardQuantityUnit, ResultTable::getRStandardQuantityUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("r_standard_quantity", ResultTable.R_STANDARD_QUANTITY_SCALE), ResultTable::setRStandardQuantity, ResultTable::getRStandardQuantity,
                IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofString("r_required_quantity_unit"), ResultTable::setRRequiredQuantityUnit, ResultTable::getRRequiredQuantityUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("r_required_quantity", ResultTable.R_REQUIRED_QUANTITY_SCALE), ResultTable::setRRequiredQuantity, ResultTable::getRRequiredQuantity,
                IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("r_unit_cost", ResultTable.R_UNIT_COST_SCALE), ResultTable::setRUnitCost, ResultTable::getRUnitCost, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("r_total_unit_cost", ResultTable.R_TOTAL_UNIT_COST_SCALE), ResultTable::setRTotalUnitCost, ResultTable::getRTotalUnitCost, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("r_manufacturing_cost", ResultTable.R_MANUFACTURING_COST_SCALE), ResultTable::setRManufacturingCost, ResultTable::getRManufacturingCost,
                IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("r_total_manufacturing_cost", ResultTable.R_TOTAL_MANUFACTURING_COST_SCALE), ResultTable::setRTotalManufacturingCost, ResultTable::getRTotalManufacturingCost,
                IceaxeRecordUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public ResultTableDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, ResultTable::new);
    }

    @Override
    public void truncate() {
        doTruncate();
    }

    @Override
    public int deleteAll() {
        return doDeleteAll();
    }

    private static final TgBindVariableInteger vFactoryId = R_F_ID.clone("factoryId");

    @Override
    public int deleteByFactory(int factoryId, LocalDate date) {
        var ps = deleteByFactoryCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(factoryId), vDate.bind(date));
        return executeAndGetCount(ps, parameter);
    }

    private final CachePreparedStatement<TgBindParameters> deleteByFactoryCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            this.sql = "delete from " + TABLE_NAME + " where r_f_id = " + vFactoryId + " and " + TG_COND_DATE;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vDate);
        }
    };

    @Override
    public int deleteByFactories(List<Integer> factoryIdList, LocalDate date) {
        var variables = TgBindVariables.of();
        var inSql = new SqlIn(R_F_ID.name());
        var parameter = TgBindParameters.of();
        int i = 0;
        for (var factoryId : factoryIdList) {
            var variable = R_F_ID.clone("id" + (i++));
            variables.add(variable);
            inSql.add(variable);
            parameter.add(variable.bind(factoryId));
        }
        variables.add(vDate);
        parameter.add(vDate.bind(date));

        var sql = "delete from " + TABLE_NAME + " where " + inSql + " and " + TG_COND_DATE;
        var parameterMapping = TgParameterMapping.of(variables);
        var session = getSession();
        try (var ps = session.createStatement(sql, parameterMapping)) {
            return executeAndGetCount(ps, parameter);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final TgBindVariableInteger vProductId = R_PRODUCT_I_ID.clone("productId");

    @Override
    public int deleteByProductId(int factoryId, LocalDate date, int productId) {
        var ps = deleteByProductIdCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(factoryId), vDate.bind(date), vProductId.bind(productId));
        return executeAndGetCount(ps, parameter);
    }

    private final CachePreparedStatement<TgBindParameters> deleteByProductIdCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            this.sql = "delete from " + TABLE_NAME + " where r_f_id = " + vFactoryId + " and " + TG_COND_DATE + " and r_product_i_id = " + vProductId;
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vDate, vProductId);
        }
    };

    @Override
    public int insert(ResultTable entity) {
        return doInsert(entity);
    }

    @Override
    public int[] insertBatch(Collection<ResultTable> entityList) {
        return doInsert(entityList);
    }

    @Override
    public List<ResultTable> selectByProductId(int factoryId, LocalDate date, int productId) {
        var ps = selectByProductIdCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(factoryId), vDate.bind(date), vProductId.bind(productId));
        return executeAndGetList(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, ResultTable> selectByProductIdCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where r_f_id = " + vFactoryId + " and " + TG_COND_DATE + " and r_product_i_id = " + vProductId //
                    + " order by r_parent_i_id, r_i_id";
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vDate, vProductId);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public Stream<ResultTable> selectRequiredQuantity(int factoryId, LocalDate date) {
        var ps = selectRequiredQuantityCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(factoryId), vDate.bind(date));
        if (BenchConst.WORKAROUND) {
            var list = executeAndGetList(ps, parameter);
            list.sort(Comparator.comparing(ResultTable::getRIId));
            return list.stream();
        }
        return executeAndGetStream(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, ResultTable> selectRequiredQuantityCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            if (BenchConst.WORKAROUND) {
                this.sql = "select" //
                        + "  r_f_id," //
                        + "  r_manufacturing_date," //
                        + "  r_i_id," //
                        + "  sum(r_required_quantity) as r_required_quantity," //
                        + "  max(r_required_quantity_unit) as r_required_quantity_unit" //
                        + " from " + TABLE_NAME + " r" //
                        + " inner join item_master m on m.i_id=r.r_i_id and m.i_effective_date<=r.r_manufacturing_date and r.r_manufacturing_date<=m.i_expired_date" //
                        + " where r_f_id=" + vFactoryId + " and r_manufacturing_date=" + vDate + " and m.i_type='raw_material'" //
                        + " group by r_f_id, r_manufacturing_date, r_i_id" //
                // + " order by r_i_id"
                ;
            } else {
                this.sql = "select" //
                        + "  r_f_id," //
                        + "  r_manufacturing_date," //
                        + "  r_i_id," //
                        + "  sum(r_required_quantity) r_required_quantity," //
                        + "  max(r_required_quantity_unit) r_required_quantity_unit" //
                        + " from " + TABLE_NAME + " r" //
                        + " left join item_master m on m.i_id=r.r_i_id and r.r_manufacturing_date between m.i_effective_date and m.i_expired_date" //
                        + " where r_f_id=" + vFactoryId + " and r_manufacturing_date=" + vDate + " and m.i_type='raw_material'" //
                        + " group by r_f_id, r_manufacturing_date, r_i_id" //
                        + " order by r_i_id";
            }
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vDate);
            this.resultMapping = TgResultMapping.of(ResultTable::new) //
                    .addInt(ResultTable::setRFId) //
                    .addDate(ResultTable::setRManufacturingDate) //
                    .addInt(ResultTable::setRIId) //
                    .addDecimal(ResultTable::setRRequiredQuantity) //
                    .addString(ResultTable::setRRequiredQuantityUnit);
        }
    };

    @Override
    public Stream<ResultTable> selectCost(int factoryId, LocalDate date) {
        var ps = selectCostCache.get();
        var parameter = TgBindParameters.of(vFactoryId.bind(factoryId), vDate.bind(date));
        return executeAndGetStream(ps, parameter);
    }

    private final CachePreparedQuery<TgBindParameters, ResultTable> selectCostCache = new CachePreparedQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql() + " where r_f_id = " + vFactoryId + " and " + TG_COND_DATE + " and r_i_id = r_product_i_id" //
                    + " order by r_product_i_id";
            this.parameterMapping = TgParameterMapping.of(vFactoryId, vDate);
            this.resultMapping = getEntityResultMapping();
        }
    };

    @Override
    public void forEach(Consumer<ResultTable> entityConsumer) {
        doForEach(entityConsumer);
    }
}
