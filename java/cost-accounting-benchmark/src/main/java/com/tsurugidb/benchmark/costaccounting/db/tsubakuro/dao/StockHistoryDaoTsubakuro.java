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
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistoryDateTime;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

public class StockHistoryDaoTsubakuro extends TsubakuroDao<StockHistory> implements StockHistoryDao {

    private static final List<TsubakuroColumn<StockHistory, ?>> COLUMN_LIST;
    static {
        List<TsubakuroColumn<StockHistory, ?>> list = new ArrayList<>();
        add(list, "s_date", AtomType.DATE, StockHistory::setSDate, StockHistory::getSDate, TsubakuroUtil::getParameter, TsubakuroUtil::getDate, true);
        add(list, "s_time", AtomType.TIME_OF_DAY, StockHistory::setSTime, StockHistory::getSTime, TsubakuroUtil::getParameter, TsubakuroUtil::getTime, true);
        add(list, "s_f_id", AtomType.INT4, StockHistory::setSFId, StockHistory::getSFId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "s_i_id", AtomType.INT4, StockHistory::setSIId, StockHistory::getSIId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "s_stock_unit", AtomType.CHARACTER, StockHistory::setSStockUnit, StockHistory::getSStockUnit, TsubakuroUtil::getParameter, TsubakuroUtil::getString);
        add(list, "s_stock_quantity", AtomType.DECIMAL, StockHistory::setSStockQuantity, StockHistory::getSStockQuantity,
                (name, value) -> TsubakuroUtil.getParameter(name, value, StockHistory.S_STOCK_QUANTITY_SCALE), TsubakuroUtil::getDecimal);
        add(list, "s_stock_amount", AtomType.DECIMAL, StockHistory::setSStockAmount, StockHistory::getSStockAmount,
                (name, value) -> TsubakuroUtil.getParameter(name, value, StockHistory.S_STOCK_AMOUNT_SCALE), TsubakuroUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public StockHistoryDaoTsubakuro(CostBenchDbManagerTsubakuro dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, StockHistory::new);
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
    public int insert(StockHistory entity) {
        return doInsert(entity, false);
    }

    @Override
    public int[] insertBatch(Collection<StockHistory> entityList) {
        return doInsert(entityList, false);
    }

    @Override
    public List<StockHistoryDateTime> selectGroupByDateTime() {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public List<StockHistoryDateTime> selectDistinctDateTime() {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public int deleteByDateTime(LocalDate date, LocalTime time) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public int deleteByDateTime(LocalDate date, LocalTime time, int factoryId) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public int deleteOldDateTime(LocalDate date, LocalTime time) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public int deleteOldDateTime(LocalDate date, LocalTime time, int factoryId) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public void insertSelectFromCostMaster(LocalDate date, LocalTime time) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public void insertSelectFromCostMaster(LocalDate date, LocalTime time, int factoryId) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public void forEach(Consumer<StockHistory> entityConsumer) {
        throw new UnsupportedOperationException("not yet impl");
    }
}
