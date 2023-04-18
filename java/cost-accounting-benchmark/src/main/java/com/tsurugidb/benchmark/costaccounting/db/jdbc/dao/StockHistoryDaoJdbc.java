package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;

public class StockHistoryDaoJdbc extends JdbcDao<StockHistory> implements StockHistoryDao {

    private static final List<JdbcColumn<StockHistory, ?>> COLUMN_LIST;
    static {
        List<JdbcColumn<StockHistory, ?>> list = new ArrayList<>();
        add(list, "s_date", StockHistory::setSDate, StockHistory::getSDate, JdbcUtil::setDate, JdbcUtil::getDate, true);
        add(list, "s_f_id", StockHistory::setSFId, StockHistory::getSFId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "s_i_id", StockHistory::setSIId, StockHistory::getSIId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "s_time", StockHistory::setSTime, StockHistory::getSTime, JdbcUtil::setTime, JdbcUtil::getTime, true);
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
