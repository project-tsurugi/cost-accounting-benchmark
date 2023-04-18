package com.tsurugidb.benchmark.costaccounting.db.dao;

import java.time.LocalDate;
import java.util.Collection;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;

public interface StockHistoryDao {

    public static final String TABLE_NAME = "stock_history";

    public static final String PS_COND_DATE = "s_date = ?";

    public static final TgBindVariable<LocalDate> vDate = BenchVariable.ofDate("date");
    public static final String TG_COND_DATE = "s_date = " + vDate.sqlName();

    /**
     * <pre>
     * truncate table stock_history
     * </pre>
     */
    void truncate();

    /**
     * <pre>
     * delete from stock_history
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * insert into stock_history
     * values(:entity)
     * </pre>
     */
    int insert(StockHistory entity);

    int[] insertBatch(Collection<StockHistory> entityList);

    void forEach(Consumer<StockHistory> entityConsumer);
}
