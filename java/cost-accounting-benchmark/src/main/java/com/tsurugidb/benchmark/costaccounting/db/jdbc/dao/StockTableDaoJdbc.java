package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

import static com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcUtil.setDate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.StockTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockTable;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;

public class StockTableDaoJdbc extends JdbcDao<StockTable> implements StockTableDao {

    private static final List<JdbcColumn<StockTable, ?>> COLUMN_LIST;
    static {
        List<JdbcColumn<StockTable, ?>> list = new ArrayList<>();
        add(list, "s_date", StockTable::setSDate, StockTable::getSDate, JdbcUtil::setDate, JdbcUtil::getDate, true);
        add(list, "s_i_id", StockTable::setSIId, StockTable::getSIId, JdbcUtil::setInt, JdbcUtil::getInt, true);
        add(list, "s_stock_unit", StockTable::setSStockUnit, StockTable::getSStockUnit, JdbcUtil::setString, JdbcUtil::getString);
        add(list, "s_stock_quantity", StockTable::setSStockQuantity, StockTable::getSStockQuantity, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        add(list, "s_stock_amount", StockTable::setSStockAmount, StockTable::getSStockAmount, JdbcUtil::setDecimal, JdbcUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public StockTableDaoJdbc(CostBenchDbManagerJdbc dbManager) {
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
    public int deleteByDate(LocalDate date) {
        String sql = "delete from " + TABLE_NAME + " where " + PS_COND_DATE;
        return executeUpdate(sql, ps -> {
            int i = 1;
            setDate(ps, i++, date);
        });
    }

    @Override
    public int insert(StockTable entity) {
        return doInsert(entity);
    }

    @Override
    public int[] insertBatch(Collection<StockTable> entityList) {
        return doInsert(entityList);
    }

    @SuppressWarnings("unused")
    private StockTable newEntity(ResultSet rs) throws SQLException {
        StockTable entity = new StockTable();
        fillEntity(entity, rs);
        return entity;
    }

    @Override
    public void forEach(Consumer<StockTable> entityConsumer) {
        doForEach(StockTable::new, entityConsumer);
    }
}
