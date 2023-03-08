package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.StockTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockTable;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;

public class StockTableDaoIceaxe extends IceaxeDao<StockTable> implements StockTableDao {

    private static final List<IceaxeColumn<StockTable, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<StockTable, ?>> list = new ArrayList<>();
        add(list, BenchVariable.ofDate("s_date"), StockTable::setSDate, StockTable::getSDate, IceaxeRecordUtil::getDate, true);
        add(list, BenchVariable.ofInt("s_i_id"), StockTable::setSIId, StockTable::getSIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofString("s_stock_unit"), StockTable::setSStockUnit, StockTable::getSStockUnit, IceaxeRecordUtil::getString);
        add(list, BenchVariable.ofDecimal("s_stock_quantity", StockTable.S_STOCK_QUANTITY_SCALE), StockTable::setSStockQuantity, StockTable::getSStockQuantity, IceaxeRecordUtil::getDecimal);
        add(list, BenchVariable.ofDecimal("s_stock_amount", StockTable.S_STOCK_AMOUNT_SCALE), StockTable::setSStockAmount, StockTable::getSStockAmount, IceaxeRecordUtil::getDecimal);
        COLUMN_LIST = list;
    }

    public StockTableDaoIceaxe(CostBenchDbManagerIceaxe dbManager) {
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
    public int deleteByDate(LocalDate date) {
        var ps = deleteByDateCache.get();
        var param = TgParameterList.of(vDate.bind(date));
        return executeAndGetCount(ps, param);
    }

    private final CachePreparedStatement<TgParameterList> deleteByDateCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            this.sql = "delete from " + TABLE_NAME + " where " + TG_COND_DATE;
            this.parameterMapping = TgParameterMapping.of(vDate);
        }
    };

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
        doForEach(entityConsumer);
    }
}
