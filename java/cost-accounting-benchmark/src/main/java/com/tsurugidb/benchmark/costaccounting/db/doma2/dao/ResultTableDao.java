package com.tsurugidb.benchmark.costaccounting.db.doma2.dao;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.seasar.doma.BatchInsert;
import org.seasar.doma.Dao;
import org.seasar.doma.Delete;
import org.seasar.doma.Insert;
import org.seasar.doma.Select;
import org.seasar.doma.Sql;
import org.seasar.doma.Suppress;
import org.seasar.doma.message.Message;

import com.tsurugidb.benchmark.costaccounting.db.doma2.config.AppConfig;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.statement.TgVariable;

@Dao(config = AppConfig.class)
public interface ResultTableDao {

    public static final String TABLE_NAME = "result_table";

    public static final String PS_COND_DATE = "r_manufacturing_date = ?";
    static final String COND_DATE = "r_manufacturing_date = /* date */'2020-09-25'";

    public static final TgVariable<LocalDate> vDate = BenchVariable.ofDate("date");
    public static final String TG_COND_DATE = "r_manufacturing_date = " + vDate.sqlName();

    @Delete
//	@Sql("delete from " + TABLE_NAME)
    @Sql("truncate table " + TABLE_NAME)
    int deleteAll();

    @Delete
    @Sql("delete from " + TABLE_NAME + " where r_f_id = /* factoryId */1 and " + COND_DATE)
    int deleteByFactory(int factoryId, LocalDate date);

    @Delete
    @Sql("delete from " + TABLE_NAME + " where r_f_id in /* factoryIdList */(1,2,3) and " + COND_DATE)
    int deleteByFactories(List<Integer> factoryIdList, LocalDate date);

    @Delete
    @Sql("delete from " + TABLE_NAME + " where r_f_id = /* factoryId */1 and " + COND_DATE + " and r_product_i_id = /* productId */2")
    int deleteByProductId(int factoryId, LocalDate date, int productId);

    @Insert
    int insert(ResultTable entity);

    @BatchInsert
    int[] insertBatch(Collection<ResultTable> entityList);

    @Select
    @Sql("select * from " + TABLE_NAME + " where r_f_id = /* factoryId */1 and " + COND_DATE + " and r_product_i_id = /* productId */2" //
            + " order by r_parent_i_id, r_i_id")
    List<ResultTable> selectByProductId(int factoryId, LocalDate date, int productId);

    @Select
    @Suppress(messages = { Message.DOMA4274 })
    Stream<ResultTable> selectRequiredQuantity(int factoryId, LocalDate date);

    @Select
    @Sql("select * from " + TABLE_NAME + " where r_f_id = /* factoryId */1 and " + COND_DATE + " and r_i_id = r_product_i_id" //
            + " order by r_product_i_id")
    @Suppress(messages = { Message.DOMA4274 })
    Stream<ResultTable> selectCost(int factoryId, LocalDate date);
}
