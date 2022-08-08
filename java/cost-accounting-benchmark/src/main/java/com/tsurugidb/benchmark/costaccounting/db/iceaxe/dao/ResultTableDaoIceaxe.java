package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;

public class ResultTableDaoIceaxe extends IceaxeDao<ResultTable> implements ResultTableDao {

    private static final TgVariableInteger R_F_ID = BenchVariable.ofInt("r_f_id");
    private static final TgVariableInteger R_PRODUCT_I_ID = BenchVariable.ofInt("r_product_i_id");
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
        add(list, BenchVariable.ofDecimal("r_weight"), ResultTable::setRWeight, ResultTable::getRWeight, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofString("r_weight_total_unit"), ResultTable::setRWeightTotalUnit, ResultTable::getRWeightTotalUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("r_weight_total"), ResultTable::setRWeightTotal, ResultTable::getRWeightTotal, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("r_weight_ratio"), ResultTable::setRWeightRatio, ResultTable::getRWeightRatio, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofString("r_standard_quantity_unit"), ResultTable::setRStandardQuantityUnit, ResultTable::getRStandardQuantityUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("r_standard_quantity"), ResultTable::setRStandardQuantity, ResultTable::getRStandardQuantity, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofString("r_required_quantity_unit"), ResultTable::setRRequiredQuantityUnit, ResultTable::getRRequiredQuantityUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("r_required_quantity"), ResultTable::setRRequiredQuantity, ResultTable::getRRequiredQuantity, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("r_unit_cost"), ResultTable::setRUnitCost, ResultTable::getRUnitCost, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("r_total_unit_cost"), ResultTable::setRTotalUnitCost, ResultTable::getRTotalUnitCost, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("r_manufacturing_cost"), ResultTable::setRManufacturingCost, ResultTable::getRManufacturingCost, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("r_total_manufacturing_cost"), ResultTable::setRTotalManufacturingCost, ResultTable::getRTotalManufacturingCost, IceaxeRecordUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public ResultTableDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, ResultTable::new);
    }

    @Override
    public int deleteAll() {
        return doDeleteAll();
    }

    private static final TgVariableInteger vFactoryId = R_F_ID.copy("factoryId");

    @Override
    public int deleteByFactory(int factoryId, LocalDate date) {
        var ps = getDeleteByFactoryPs();
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vDate.bind(date));
        return executeAndGetCount(ps, param);
    }

    private TsurugiPreparedStatementUpdate1<TgParameterList> deleteByFactoryPs;

    private TsurugiPreparedStatementUpdate1<TgParameterList> getDeleteByFactoryPs() {
        if (this.deleteByFactoryPs == null) {
            var sql = "delete from " + TABLE_NAME + " where r_f_id = " + vFactoryId + " and " + TG_COND_DATE;
            var parameterMapping = TgParameterMapping.of(vFactoryId, vDate);
            this.deleteByFactoryPs = createPreparedStatement(sql, parameterMapping);
        }
        return this.deleteByFactoryPs;
    }

    @Override
    public int deleteByFactories(List<Integer> factoryIdList, LocalDate date) {
        var vlist = TgVariableList.of();
        var inSql = new SqlIn(R_F_ID.name());
        var param = TgParameterList.of();
        int i = 0;
        for (var factoryId : factoryIdList) {
            var variable = R_F_ID.copy("id" + (i++));
            vlist.add(variable);
            inSql.add(variable);
            param.add(variable.bind(factoryId));
        }
        vlist.add(vDate);
        param.add(vDate.bind(date));

        var sql = "delete from " + TABLE_NAME + " where " + inSql + " and " + TG_COND_DATE;
        var parameterMapping = TgParameterMapping.of(vlist);
        try (var ps = createPreparedStatement(sql, parameterMapping)) {
            return executeAndGetCount(ps, param);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final TgVariableInteger vProductId = R_PRODUCT_I_ID.copy("productId");

    @Override
    public int deleteByProductId(int factoryId, LocalDate date, int productId) {
        var ps = getDeleteByProductIdPs();
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vDate.bind(date), vProductId.bind(productId));
        return executeAndGetCount(ps, param);
    }

    private TsurugiPreparedStatementUpdate1<TgParameterList> deleteByProductIdPs;

    private TsurugiPreparedStatementUpdate1<TgParameterList> getDeleteByProductIdPs() {
        if (this.deleteByProductIdPs == null) {
            var sql = "delete from " + TABLE_NAME + " where r_f_id = " + vFactoryId + " and " + TG_COND_DATE + " and r_product_i_id = " + vProductId;
            var parameterMapping = TgParameterMapping.of(vFactoryId, vDate, vProductId);
            this.deleteByProductIdPs = createPreparedStatement(sql, parameterMapping);
        }
        return this.deleteByProductIdPs;
    }

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
        var ps = getSelectByProductIdPs();
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vDate.bind(date), vProductId.bind(productId));
        return executeAndGetList(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ResultTable> selectByProductIdPs;

    private TsurugiPreparedStatementQuery1<TgParameterList, ResultTable> getSelectByProductIdPs() {
        if (this.selectByProductIdPs == null) {
            var sql = getSelectEntitySql() + " where r_f_id = " + vFactoryId + " and " + TG_COND_DATE + " and r_product_i_id = " + vProductId //
                    + " order by r_parent_i_id, r_i_id";
            var parameterMapping = TgParameterMapping.of(vFactoryId, vDate, vProductId);
            var resultMapping = getEntityResultMapping();
            this.selectByProductIdPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectByProductIdPs;
    }

    @Override
    public Stream<ResultTable> selectRequiredQuantity(int factoryId, LocalDate date) {
        var ps = getSelectRequiredQuantityPs();
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vDate.bind(date));
        return executeAndGetStream(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ResultTable> selectRequiredQuantityPs;

    private TsurugiPreparedStatementQuery1<TgParameterList, ResultTable> getSelectRequiredQuantityPs() {
        if (this.selectRequiredQuantityPs == null) {
            var sql = "select" //
                    + "  r_f_id," //
                    + "  r_manufacturing_date," //
                    + "  r_i_id," //
                    + "  sum(r_required_quantity) r_required_quantity," //
                    + "  max(r_required_quantity_unit) r_required_quantity_unit" //
                    + " from " + TABLE_NAME + " r" //
                    + " left join item_master m on m.i_id=r.r_i_id" //
                    + " where r_f_id=" + vFactoryId + " and r_manufacturing_date=" + vDate + " and m.i_type='raw_material'" //
                    + " group by r_f_id, r_manufacturing_date, r_i_id" //
                    + " order by r_i_id";
            var parameterMapping = TgParameterMapping.of(vFactoryId, vDate);
            var resultMapping = TgResultMapping.of(ResultTable::new) //
                    .int4(ResultTable::setRFId) //
                    .date(ResultTable::setRManufacturingDate) //
                    .int4(ResultTable::setRIId) //
                    .decimal(ResultTable::setRRequiredQuantity) //
                    .character(ResultTable::setRRequiredQuantityUnit);
            this.selectRequiredQuantityPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectRequiredQuantityPs;
    }

    @Override
    public Stream<ResultTable> selectCost(int factoryId, LocalDate date) {
        var ps = getSelectCostPs();
        var param = TgParameterList.of(vFactoryId.bind(factoryId), vDate.bind(date));
        return executeAndGetStream(ps, param);
    }

    private TsurugiPreparedStatementQuery1<TgParameterList, ResultTable> selectCostPs;

    private TsurugiPreparedStatementQuery1<TgParameterList, ResultTable> getSelectCostPs() {
        if (this.selectCostPs == null) {
            var sql = getSelectEntitySql() + " where r_f_id = " + vFactoryId + " and " + TG_COND_DATE + " and r_i_id = r_product_i_id" //
                    + " order by r_product_i_id";
            var parameterMapping = TgParameterMapping.of(vFactoryId, vDate);
            var resultMapping = getEntityResultMapping();
            this.selectCostPs = createPreparedQuery(sql, parameterMapping, resultMapping);
        }
        return this.selectCostPs;
    }
}
