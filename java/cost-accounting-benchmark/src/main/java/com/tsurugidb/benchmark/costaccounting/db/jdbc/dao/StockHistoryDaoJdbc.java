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
package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistoryDateTime;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class StockHistoryDaoJdbc extends JdbcDao<StockHistory> implements StockHistoryDao {

    private static final List<JdbcColumn<StockHistory, ?>> COLUMN_LIST;
    static {
        List<JdbcColumn<StockHistory, ?>> list = new ArrayList<>();
        add(list, "s_date", StockHistory::setSDate, StockHistory::getSDate, JdbcUtil::setDate, JdbcUtil::getDate, true);
        add(list, "s_time", StockHistory::setSTime, StockHistory::getSTime, JdbcUtil::setTime, JdbcUtil::getTime, true);
        add(list, "s_f_id", StockHistory::setSFId, StockHistory::getSFId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "s_i_id", StockHistory::setSIId, StockHistory::getSIId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "s_stock_unit", StockHistory::setSStockUnit, StockHistory::getSStockUnit, JdbcUtil::setString, JdbcUtil::getString);
        add(list, "s_stock_quantity", StockHistory::setSStockQuantity, StockHistory::getSStockQuantity, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "s_stock_amount", StockHistory::setSStockAmount, StockHistory::getSStockAmount, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public StockHistoryDaoJdbc(CostBenchDbManagerJdbc dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST);
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
        return doInsert(entity);
    }

    @Override
    public int[] insertBatch(Collection<StockHistory> entityList) {
        return doInsert(entityList);
    }

    @Override
    public List<StockHistoryDateTime> selectGroupByDateTime() {
        String sql = "select s_date, s_time from " + TABLE_NAME //
                + " group by s_date, s_time" //
                + " order by s_date, s_time";
        return executeQueryList(sql, ps -> {
        }, rs -> {
            var entity = new StockHistoryDateTime();
            entity.setSDate(JdbcUtil.getDate(rs, "s_date"));
            entity.setSTime(JdbcUtil.getTime(rs, "s_time"));
            return entity;
        });
    }

    @Override
    public List<StockHistoryDateTime> selectDistinctDateTime() {
        String sql = "select distinct s_date, s_time from " + TABLE_NAME //
                + " order by s_date, s_time";
        return executeQueryList(sql, ps -> {
        }, rs -> {
            var entity = new StockHistoryDateTime();
            entity.setSDate(JdbcUtil.getDate(rs, "s_date"));
            entity.setSTime(JdbcUtil.getTime(rs, "s_time"));
            return entity;
        });
    }

    @Override
    public int deleteByDateTime(LocalDate date, LocalTime time) {
        String sql = "delete from " + TABLE_NAME //
                + " where s_date = ? and s_time = ?";
        return executeUpdate(sql, ps -> {
            int i = 1;
            JdbcUtil.setDate(ps, i++, date);
            JdbcUtil.setTime(ps, i++, time);
        });
    }

    @Override
    public int deleteByDateTime(LocalDate date, LocalTime time, int factoryId) {
        String sql = "delete from " + TABLE_NAME //
                + " where s_date = ? and s_time = ? and s_f_id = ?";
        return executeUpdate(sql, ps -> {
            int i = 1;
            JdbcUtil.setDate(ps, i++, date);
            JdbcUtil.setTime(ps, i++, time);
            JdbcUtil.setInt(ps, i++, factoryId);
        });
    }

    @Override
    public int deleteOldDateTime(LocalDate date, LocalTime time) {
        String where = "(s_date < ?) or (s_date = ? and s_time <= ?)";
        if (dbManager.isTsurugi() && BenchConst.WORKAROUND) { // orでつなぐとfull scanになる為
            where = "(" + where + ") and s_date <= ?";
        }
        String sql = "delete from " + TABLE_NAME //
                + " where " + where;
        return executeUpdate(sql, ps -> {
            int i = 1;
            JdbcUtil.setDate(ps, i++, date);
            JdbcUtil.setDate(ps, i++, date);
            JdbcUtil.setTime(ps, i++, time);
            if (dbManager.isTsurugi() && BenchConst.WORKAROUND) {
                JdbcUtil.setDate(ps, i++, date);
            }
        });
    }

    @Override
    public int deleteOldDateTime(LocalDate date, LocalTime time, int factoryId) {
        String where = "((s_date < ?) or (s_date = ? and s_time <= ?)) and s_f_id = ?";
        if (dbManager.isTsurugi() && BenchConst.WORKAROUND) { // orでつなぐとfull scanになる為
            where = "(" + where + ") and s_date <= ?";
        }
        String sql = "delete from " + TABLE_NAME //
                + " where " + where;
        return executeUpdate(sql, ps -> {
            int i = 1;
            JdbcUtil.setDate(ps, i++, date);
            JdbcUtil.setDate(ps, i++, date);
            JdbcUtil.setTime(ps, i++, time);
            JdbcUtil.setInt(ps, i++, factoryId);
            if (dbManager.isTsurugi() && BenchConst.WORKAROUND) {
                JdbcUtil.setDate(ps, i++, date);
            }
        });
    }

    private static final String INSERT_SELECT_FROM_COST_MASTER_SQL = "insert into " + TABLE_NAME //
            + "(" + COLUMN_LIST.stream().map(c -> c.getName()).collect(Collectors.joining(", ")) + ")" //
            + " select ?, ?, c_f_id, c_i_id, c_stock_unit, c_stock_quantity, c_stock_amount from " + CostMasterDao.TABLE_NAME;

    @Override
    public void insertSelectFromCostMaster(LocalDate date, LocalTime time) {
        executeUpdate(INSERT_SELECT_FROM_COST_MASTER_SQL, ps -> {
            int i = 1;
            JdbcUtil.setDate(ps, i++, date);
            JdbcUtil.setTime(ps, i++, time);
        });
    }

    private static final String INSERT_SELECT_FROM_COST_MASTER_FACTORY_SQL = INSERT_SELECT_FROM_COST_MASTER_SQL + " where c_f_id = ?";

    @Override
    public void insertSelectFromCostMaster(LocalDate date, LocalTime time, int factoryId) {
        executeUpdate(INSERT_SELECT_FROM_COST_MASTER_FACTORY_SQL, ps -> {
            int i = 1;
            JdbcUtil.setDate(ps, i++, date);
            JdbcUtil.setTime(ps, i++, time);
            JdbcUtil.setInt(ps, i++, factoryId);
        });
    }

    @SuppressWarnings("unused")
    private StockHistory newEntity(ResultSet rs) throws SQLException {
        StockHistory entity = new StockHistory();
        fillEntity(entity, rs);
        return entity;
    }

    @Override
    public void forEach(Consumer<StockHistory> entityConsumer) {
        doForEach(StockHistory::new, entityConsumer);
    }
}
