/*
 * Copyright 2023-2024 Project Tsurugi.
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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;

public class ResultTableDaoTsubakuro extends TsubakuroDao<ResultTable> implements ResultTableDao {

    private static final List<TsubakuroColumn<ResultTable, ?>> COLUMN_LIST;
    static {
        List<TsubakuroColumn<ResultTable, ?>> list = new ArrayList<>();
        add(list, "r_f_id", AtomType.INT4, ResultTable::setRFId, ResultTable::getRFId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "r_manufacturing_date", AtomType.DATE, ResultTable::setRManufacturingDate, ResultTable::getRManufacturingDate, TsubakuroUtil::getParameter, TsubakuroUtil::getDate, true);
        add(list, "r_product_i_id", AtomType.INT4, ResultTable::setRProductIId, ResultTable::getRProductIId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "r_parent_i_id", AtomType.INT4, ResultTable::setRParentIId, ResultTable::getRParentIId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "r_i_id", AtomType.INT4, ResultTable::setRIId, ResultTable::getRIId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "r_manufacturing_quantity", AtomType.DECIMAL, ResultTable::setRManufacturingQuantity, ResultTable::getRManufacturingQuantity, TsubakuroUtil::getParameter, TsubakuroUtil::getBigInt);
        add(list, "r_weight_unit", AtomType.CHARACTER, ResultTable::setRWeightUnit, ResultTable::getRWeightUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "r_weight", AtomType.DECIMAL, ResultTable::setRWeight, ResultTable::getRWeight, (name, value) -> TsubakuroUtil.getParameter(name, value, ResultTable.R_WEIGHT_SCALE),
                TsubakuroUtil::getDecimal);
        add(list, "r_weight_total_unit", AtomType.CHARACTER, ResultTable::setRWeightTotalUnit, ResultTable::getRWeightTotalUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "r_weight_total", AtomType.DECIMAL, ResultTable::setRWeightTotal, ResultTable::getRWeightTotal,
                (name, value) -> TsubakuroUtil.getParameter(name, value, ResultTable.R_WEIGHT_TOTAL_SCALE), TsubakuroUtil::getDecimal);
        add(list, "r_weight_ratio", AtomType.DECIMAL, ResultTable::setRWeightRatio, ResultTable::getRWeightRatio,
                (name, value) -> TsubakuroUtil.getParameter(name, value, ResultTable.R_WEIGHT_RATIO_SCALE), TsubakuroUtil::getDecimal);
        add(list, "r_standard_quantity_unit", AtomType.CHARACTER, ResultTable::setRStandardQuantityUnit, ResultTable::getRStandardQuantityUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "r_standard_quantity", AtomType.DECIMAL, ResultTable::setRStandardQuantity, ResultTable::getRStandardQuantity,
                (name, value) -> TsubakuroUtil.getParameter(name, value, ResultTable.R_STANDARD_QUANTITY_SCALE), TsubakuroUtil::getDecimal);
        add(list, "r_required_quantity_unit", AtomType.CHARACTER, ResultTable::setRRequiredQuantityUnit, ResultTable::getRRequiredQuantityUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "r_required_quantity", AtomType.DECIMAL, ResultTable::setRRequiredQuantity, ResultTable::getRRequiredQuantity,
                (name, value) -> TsubakuroUtil.getParameter(name, value, ResultTable.R_REQUIRED_QUANTITY_SCALE), TsubakuroUtil::getDecimal);
        add(list, "r_unit_cost", AtomType.DECIMAL, ResultTable::setRUnitCost, ResultTable::getRUnitCost, (name, value) -> TsubakuroUtil.getParameter(name, value, ResultTable.R_UNIT_COST_SCALE),
                TsubakuroUtil::getDecimal);
        add(list, "r_total_unit_cost", AtomType.DECIMAL, ResultTable::setRTotalUnitCost, ResultTable::getRTotalUnitCost,
                (name, value) -> TsubakuroUtil.getParameter(name, value, ResultTable.R_TOTAL_UNIT_COST_SCALE), TsubakuroUtil::getDecimal);
        add(list, "r_manufacturing_cost", AtomType.DECIMAL, ResultTable::setRManufacturingCost, ResultTable::getRManufacturingCost,
                (name, value) -> TsubakuroUtil.getParameter(name, value, ResultTable.R_MANUFACTURING_COST_SCALE), TsubakuroUtil::getDecimal);
        add(list, "r_total_manufacturing_cost", AtomType.DECIMAL, ResultTable::setRTotalManufacturingCost, ResultTable::getRTotalManufacturingCost,
                (name, value) -> TsubakuroUtil.getParameter(name, value, ResultTable.R_TOTAL_MANUFACTURING_COST_SCALE), TsubakuroUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public ResultTableDaoTsubakuro(CostBenchDbManagerTsubakuro dbManager) {
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

    @Override
    public int deleteByFactory(int factoryId, LocalDate date) {
        var ps = getSelectIdByTypePs();
        var parameters = List.of( //
                Parameters.of("factoryId", factoryId), //
                Parameters.of(vDate.name(), date));
        explain(deleteByFactorySql, ps, parameters);
        return executeAndGetCount(ps, parameters);
    }

    private String deleteByFactorySql;
    private PreparedStatement deleteByFactoryPs;

    private synchronized PreparedStatement getSelectIdByTypePs() {
        if (this.deleteByFactoryPs == null) {
            this.deleteByFactorySql = "delete from " + TABLE_NAME + " where r_f_id = :factoryId and " + TG_COND_DATE;
            var placeholders = List.of( //
                    Placeholders.of("factoryId", AtomType.INT4), //
                    Placeholders.of(vDate.name(), AtomType.DATE));
            this.deleteByFactoryPs = createPreparedStatement(deleteByFactorySql, placeholders);
        }
        return this.deleteByFactoryPs;
    }

    @Override
    public int deleteByFactories(List<Integer> factoryIdList, LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public int deleteByProductId(int factoryId, LocalDate date, int productId) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public int insert(ResultTable entity) {
        return doInsert(entity, false);
    }

    @Override
    public int[] insertBatch(Collection<ResultTable> entityList) {
        return doInsert(entityList, false);
    }

    @Override
    public List<ResultTable> selectByProductId(int factoryId, LocalDate date, int productId) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public Stream<ResultTable> selectRequiredQuantity(int factoryId, LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public Stream<ResultTable> selectCost(int factoryId, LocalDate date) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public void forEach(Consumer<ResultTable> entityConsumer) {
        throw new UnsupportedOperationException("not yet impl");
    }
}
