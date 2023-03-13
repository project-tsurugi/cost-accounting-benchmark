package com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.StockTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockTable;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

public class StockTableDaoTsubakuro extends TsubakuroDao<StockTable> implements StockTableDao {

    private static final List<TsubakuroColumn<StockTable, ?>> COLUMN_LIST;
    static {
        List<TsubakuroColumn<StockTable, ?>> list = new ArrayList<>();
        add(list, "s_date", AtomType.DATE, StockTable::setSDate, StockTable::getSDate, TsubakuroUtil::getParameter, TsubakuroUtil::getDate, true);
        add(list, "s_f_id", AtomType.INT4, StockTable::setSFId, StockTable::getSFId, TsubakuroUtil::getParameter, TsubakuroUtil::getInt, true);
        add(list, "s_stock_amount", AtomType.DECIMAL, StockTable::setSStockAmount, StockTable::getSStockAmount,
                (name, value) -> TsubakuroUtil.getParameter(name, value, StockTable.S_STOCK_AMOUNT_SCALE), TsubakuroUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public StockTableDaoTsubakuro(CostBenchDbManagerTsubakuro dbManager) {
        super(dbManager, TABLE_NAME, COLUMN_LIST, StockTable::new);
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
    public int deleteByDateFactory(LocalDate date, int fId) {
        throw new UnsupportedOperationException("not yet impl");
    }

    @Override
    public int insert(StockTable entity) {
        return doInsert(entity);
    }

    @Override
    public int[] insertBatch(Collection<StockTable> entityList) {
        return doInsert(entityList);
    }

    @Override
    public void forEach(Consumer<StockTable> entityConsumer) {
        throw new UnsupportedOperationException("not yet impl");
    }
}
