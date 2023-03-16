package com.tsurugidb.benchmark.costaccounting.db.dao;

import java.time.LocalDate;
import java.util.Collection;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.entity.StockTable;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;

public interface StockTableDao {

    public static final String TABLE_NAME = "stock_table";

    public static final String PS_COND_DATE = "s_date = ?";

    public static final TgBindVariable<LocalDate> vDate = BenchVariable.ofDate("date");
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
     * where s_date = :date and s_f_id = :fId
     * </pre>
     */
    int deleteByDateFactory(LocalDate date, int fId);

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
