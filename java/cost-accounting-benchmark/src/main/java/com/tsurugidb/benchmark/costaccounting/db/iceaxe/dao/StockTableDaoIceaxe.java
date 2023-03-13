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
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;

public class StockTableDaoIceaxe extends IceaxeDao<StockTable> implements StockTableDao {

    private static final TgVariableInteger S_F_ID = BenchVariable.ofInt("s_f_id");
    private static final List<IceaxeColumn<StockTable, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<StockTable, ?>> list = new ArrayList<>();
        add(list, BenchVariable.ofDate("s_date"), StockTable::setSDate, StockTable::getSDate, IceaxeRecordUtil::getDate, true);
        add(list, S_F_ID, StockTable::setSFId, StockTable::getSFId, IceaxeRecordUtil::getInt, true);
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

    private static final TgVariableInteger vFactoryId = S_F_ID.copy("fId");

    @Override
    public int deleteByDateFactory(LocalDate date, int fId) {
        var ps = deleteByDateCache.get();
        var param = TgParameterList.of(vDate.bind(date), vFactoryId.bind(fId));
        return executeAndGetCount(ps, param);
    }

    private final CachePreparedStatement<TgParameterList> deleteByDateCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            this.sql = "delete from " + TABLE_NAME + " where " + TG_COND_DATE + " and s_f_id = " + vFactoryId;
            this.parameterMapping = TgParameterMapping.of(vDate, vFactoryId);
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
