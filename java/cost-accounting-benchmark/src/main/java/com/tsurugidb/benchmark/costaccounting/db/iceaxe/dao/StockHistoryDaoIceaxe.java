package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;

public class StockHistoryDaoIceaxe extends IceaxeDao<StockHistory> implements StockHistoryDao {

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
    public void insertSelectFromCostMaster(LocalDate date, LocalTime time) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public void insertSelectFromCostMaster(LocalDate date, LocalTime time, int factoryId) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public void forEach(Consumer<StockHistory> entityConsumer) {
        doForEach(entityConsumer);
    }
}
