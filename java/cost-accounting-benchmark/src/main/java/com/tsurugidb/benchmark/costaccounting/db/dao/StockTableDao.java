package com.tsurugidb.benchmark.costaccounting.db.dao;

import java.time.LocalDate;
import java.util.Collection;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.entity.StockTable;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.statement.TgVariable;

public interface StockTableDao {

    public static final String TABLE_NAME = "stock_table";

    public static final String PS_COND_DATE = "s_date = ?";

    public static final TgVariable<LocalDate> vDate = BenchVariable.ofDate("date");
    public static final String TG_COND_DATE = "s_date = " + vDate.sqlName();

    /**
     * <pre>
     * truncate table stock_table
     * </pre>
     */
    void truncate();

    /**
     * <pre>
     * delete from stock_table
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * delete from stock_table
     * where s_date = :date
     * </pre>
     */
    int deleteByDate(LocalDate date);

    /**
     * <pre>
     * insert into stock_table
     * values(:entity)
     * </pre>
     */
    int insert(StockTable entity);

    int[] insertBatch(Collection<StockTable> entityList);

    void forEach(Consumer<StockTable> entityConsumer);
}
