package com.tsurugidb.benchmark.costaccounting.db.dao;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.statement.TgVariable;

public interface ResultTableDao {

    public static final String TABLE_NAME = "result_table";

    public static final String PS_COND_DATE = "r_manufacturing_date = ?";

    public static final TgVariable<LocalDate> vDate = BenchVariable.ofDate("date");
    public static final String TG_COND_DATE = "r_manufacturing_date = " + vDate.sqlName();

    /**
     * <pre>
     * delete from result_table
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * delete from result_table
     * where r_f_id = :factoryId
     *   and r_manufacturing_date = :date
     * </pre>
     */
    int deleteByFactory(int factoryId, LocalDate date);

    /**
     * <pre>
     * delete from result_table
     * where r_f_id in (:factoryIdList)
     *   and r_manufacturing_date = :date
     * </pre>
     */
    int deleteByFactories(List<Integer> factoryIdList, LocalDate date);

    /**
     * <pre>
     * delete from result_table
     * where r_f_id = :factoryId
     *   and r_manufacturing_date = :date
     *   and r_product_i_id = :productId
     * </pre>
     */
    int deleteByProductId(int factoryId, LocalDate date, int productId);

    /**
     * <pre>
     * insert into result_table
     * values(:entity)
     * </pre>
     */
    int insert(ResultTable entity);

    int[] insertBatch(Collection<ResultTable> entityList);

    /**
     * <pre>
     * select * from result_table
     * where r_f_id = :factoryId
     *   and r_manufacturing_date = :date
     *   and r_product_i_id = :productId
     * order by r_parent_i_id, r_i_id
     * </pre>
     */
    List<ResultTable> selectByProductId(int factoryId, LocalDate date, int productId);

    /**
     * <pre>
     * select
     *   r_f_id,
     *   r_manufacturing_date,
     *   r_i_id,
     *   sum(r_required_quantity) r_required_quantity,
     *   max(r_required_quantity_unit) r_required_quantity_unit
     * from result_table r
     * left join item_master m on m.i_id=r.r_i_id
     * where r_f_id = :factoryId
     *   and r_manufacturing_date = :date
     *   and m.i_type = 'raw_material'
     * group by r_f_id, r_manufacturing_date, r_i_id
     * order by r_i_id
     * </pre>
     */
    Stream<ResultTable> selectRequiredQuantity(int factoryId, LocalDate date);

    /**
     * <pre>
     * select * from result_table
     * where r_f_id = :factoryId
     *   and r_manufacturing_date = :date
     *   and r_i_id = r_product_i_id
     *   order by r_product_i_id
     * </pre>
     */
    Stream<ResultTable> selectCost(int factoryId, LocalDate date);

    void forEach(Consumer<ResultTable> entityConsumer);
}
