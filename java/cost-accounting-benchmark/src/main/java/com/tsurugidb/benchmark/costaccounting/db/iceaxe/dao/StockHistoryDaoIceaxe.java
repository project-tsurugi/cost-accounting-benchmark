package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableInteger;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;

public class StockHistoryDaoIceaxe extends IceaxeDao<StockHistory> implements StockHistoryDao {

    private static final TgBindVariableInteger S_F_ID = BenchVariable.ofInt("s_f_id");
    private static final List<IceaxeColumn<StockHistory, ?>> COLUMN_LIST;
    static {
        List<IceaxeColumn<StockHistory, ?>> list = new ArrayList<>();
        add(list, BenchVariable.ofDate("s_date"), StockHistory::setSDate, StockHistory::getSDate, IceaxeRecordUtil::getDate, true);
        add(list, S_F_ID, StockHistory::setSFId, StockHistory::getSFId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofInt("s_i_id"), StockHistory::setSIId, StockHistory::getSIId, IceaxeRecordUtil::getInt, true);
        add(list, BenchVariable.ofTime("s_time"), StockHistory::setSTime, StockHistory::getSTime, IceaxeRecordUtil::getTime, true);
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

    private static final TgBindVariableInteger vFactoryId = S_F_ID.clone("fId");

    @Override
    public int deleteByDateFactory(LocalDate date, int fId) {
        var ps = deleteByDateCache.get();
        var parameter = TgBindParameters.of(vDate.bind(date), vFactoryId.bind(fId));
        return executeAndGetCount(ps, parameter);
    }

    private final CachePreparedStatement<TgBindParameters> deleteByDateCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            this.sql = "delete from " + TABLE_NAME + " where " + TG_COND_DATE + " and s_f_id = " + vFactoryId;
            this.parameterMapping = TgParameterMapping.of(vDate, vFactoryId);
        }
    };

    @Override
    public int insert(StockHistory entity) {
        return doInsert(entity);
    }

    @Override
    public int[] insertBatch(Collection<StockHistory> entityList) {
        return doInsert(entityList);
    }

    @Override
    public void forEach(Consumer<StockHistory> entityConsumer) {
        doForEach(entityConsumer);
    }
}
