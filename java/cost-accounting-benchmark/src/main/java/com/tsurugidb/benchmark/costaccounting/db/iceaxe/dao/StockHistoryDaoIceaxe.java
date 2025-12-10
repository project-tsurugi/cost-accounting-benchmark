/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistoryDateTime;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableInteger;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;

public class StockHistoryDaoIceaxe extends IceaxeDao<StockHistory> implements StockHistoryDao {

    public static final TgBindVariable<LocalDate> vDate = BenchVariable.ofDate("date");
    public static final TgBindVariable<LocalTime> vTime = BenchVariable.ofTime("time");
    public static final TgBindVariableInteger vFactory = BenchVariable.ofInt("fId");

    private static final List<IceaxeColumn<StockHistory, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<StockHistory, ?>> list = new ArrayList<>();
        add(list, BenchVariable.ofDate("s_date"), StockHistory::setSDate, StockHistory::getSDate, IceaxeRecordUtil::getDate, true);
        add(list, BenchVariable.ofTime("s_time"), StockHistory::setSTime, StockHistory::getSTime, IceaxeRecordUtil::getTime, true);
        add(list, BenchVariable.ofInt("s_f_id"), StockHistory::setSFId, StockHistory::getSFId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofInt("s_i_id"), StockHistory::setSIId, StockHistory::getSIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofString("s_stock_unit"), StockHistory::setSStockUnit, StockHistory::getSStockUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("s_stock_quantity", StockHistory.S_STOCK_QUANTITY_SCALE), StockHistory::setSStockQuantity, StockHistory::getSStockQuantity, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("s_stock_amount", StockHistory.S_STOCK_AMOUNT_SCALE), StockHistory::setSStockAmount, StockHistory::getSStockAmount, IceaxeRecordUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public StockHistoryDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
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
        var ps = selectGroupByDateTimeCache.get();
        var list = executeAndGetList(ps);
        return list;
    }

    private final CacheQuery<StockHistoryDateTime> selectGroupByDateTimeCache = new CacheQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select s_date, s_time from " + TABLE_NAME //
                    + " group by s_date, s_time" //
                    + " order by s_date, s_time";
            this.resultMapping = TgResultMapping.of(StockHistoryDateTime::new) //
                    .addDate("s_date", StockHistoryDateTime::setSDate) //
                    .addTime("s_time", StockHistoryDateTime::setSTime);
        }
    };

    @Override
    public List<StockHistoryDateTime> selectDistinctDateTime() {
        var ps = selectDistinctDateTimeCache.get();
        var list = executeAndGetList(ps);
        return list;
    }

    private final CacheQuery<StockHistoryDateTime> selectDistinctDateTimeCache = new CacheQuery<>() {
        @Override
        protected void initialize() {
            this.sql = "select distinct s_date, s_time from " + TABLE_NAME //
                    + " order by s_date, s_time";
            this.resultMapping = TgResultMapping.of(StockHistoryDateTime::new) //
                    .addDate("s_date", StockHistoryDateTime::setSDate) //
                    .addTime("s_time", StockHistoryDateTime::setSTime);
        }
    };

    @Override
    public int deleteByDateTime(LocalDate date, LocalTime time) {
        var ps = deleteByDateTimeCache.get();
        var parameter = TgBindParameters.of(vDate.bind(date), vTime.bind(time));
        return executeAndGetCount(ps, parameter);
    }

    private final CachePreparedStatement<TgBindParameters> deleteByDateTimeCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            this.sql = "delete from " + TABLE_NAME //
                    + " where s_date = " + vDate + " and s_time = " + vTime;
            this.parameterMapping = TgParameterMapping.of(vDate, vTime);
        }
    };

    @Override
    public int deleteByDateTime(LocalDate date, LocalTime time, int factoryId) {
        var ps = deleteByDateTimeFactoryCache.get();
        var parameter = TgBindParameters.of(vDate.bind(date), vTime.bind(time), vFactory.bind(factoryId));
        return executeAndGetCount(ps, parameter);
    }

    private final CachePreparedStatement<TgBindParameters> deleteByDateTimeFactoryCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            this.sql = "delete from " + TABLE_NAME //
                    + " where s_date = " + vDate + " and s_time = " + vTime + " and s_f_id = " + vFactory;
            this.parameterMapping = TgParameterMapping.of(vDate, vTime, vFactory);
        }
    };

    @Override
    public int deleteOldDateTime(LocalDate date, LocalTime time) {
        var ps = deleteOldDateTimeCache.get();
        var parameter = TgBindParameters.of(vDate.bind(date), vTime.bind(time));
        return executeAndGetCount(ps, parameter);
    }

    private final CachePreparedStatement<TgBindParameters> deleteOldDateTimeCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            String where = "(s_date < " + vDate + ") or (s_date = " + vDate + " and s_time <= " + vTime + ")";
            this.sql = "delete from " + TABLE_NAME + " where " + where;
            this.parameterMapping = TgParameterMapping.of(vDate, vTime);
        }
    };

    @Override
    public int deleteOldDateTime(LocalDate date, LocalTime time, int factoryId) {
        var ps = deleteOldDateTimeFactoryCache.get();
        var parameter = TgBindParameters.of(vDate.bind(date), vTime.bind(time), vFactory.bind(factoryId));
        return executeAndGetCount(ps, parameter);
    }

    private final CachePreparedStatement<TgBindParameters> deleteOldDateTimeFactoryCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            String where = "((s_date < " + vDate + ") or (s_date = " + vDate + " and s_time <= " + vTime + ")) and s_f_id = " + vFactory;
            this.sql = "delete from " + TABLE_NAME + " where " + where;
            this.parameterMapping = TgParameterMapping.of(vDate, vTime, vFactory);
        }
    };

    @Override
    public void insertSelectFromCostMaster(LocalDate date, LocalTime time) {
        var ps = insertSelectFromCostMasterCache.get();
        var parameter = TgBindParameters.of(vDate.bind(date), vTime.bind(time));
        executeAndGetCount(ps, parameter);
    }

    private final CachePreparedStatement<TgBindParameters> insertSelectFromCostMasterCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            var names = getColumnNames();
            this.sql = insert + " into " + TABLE_NAME //
                    + "(" + names + ")" //
                    + " select " + vDate + ", " + vTime + ", c_f_id, c_i_id, c_stock_unit, c_stock_quantity, c_stock_amount from " + CostMasterDao.TABLE_NAME;
            this.parameterMapping = TgParameterMapping.of(vDate, vTime);
        }
    };

    @Override
    public void insertSelectFromCostMaster(LocalDate date, LocalTime time, int factoryId) {
        var ps = insertSelectFromCostMasterByFactoryCache.get();
        var parameter = TgBindParameters.of(vDate.bind(date), vTime.bind(time), vFactory.bind(factoryId));
        executeAndGetCount(ps, parameter);
    }

    private final CachePreparedStatement<TgBindParameters> insertSelectFromCostMasterByFactoryCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            var names = getColumnNames();
            this.sql = insert + " into " + TABLE_NAME //
                    + "(" + names + ")" //
                    + " select " + vDate + ", " + vTime + ", c_f_id, c_i_id, c_stock_unit, c_stock_quantity, c_stock_amount from " + CostMasterDao.TABLE_NAME + " where c_f_id=" + vFactory;
            this.parameterMapping = TgParameterMapping.of(vDate, vTime, vFactory);
        }
    };

    @Override
    public void forEach(Consumer<StockHistory> entityConsumer) {
        doForEach(entityConsumer);
    }
}
