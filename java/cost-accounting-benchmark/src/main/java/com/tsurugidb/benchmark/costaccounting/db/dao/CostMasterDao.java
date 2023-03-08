package com.tsurugidb.benchmark.costaccounting.db.dao;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;

public interface CostMasterDao {

    public static final String TABLE_NAME = "cost_master";

    /**
     * <pre>
     * truncate table cost_master
     * </pre>
     */
    void truncate();

    /**
     * <pre>
     * delete from cost_master
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * insert into cost_master
     * values(:entity)
     * </pre>
     */
    int insert(CostMaster entity);

    /**
     * <pre>
     * select * from cost_master
     * where c_f_id = :fId
     * </pre>
     */
    List<CostMaster> selectByFactory(int fId);

    /**
     * <pre>
     * select * from cost_master
     * where c_f_id = :fId and c_i_id = :iId
     * </pre>
     */
    CostMaster selectById(int fId, int iId);

    /**
     * <pre>
     * select * from cost_master
     * where c_f_id = :in.cFId and c_i_id = :in.cIId
     * for update
     * </pre>
     */
    CostMaster lock(CostMaster in);

    /**
     * <pre>
     * select * from cost_master
     * order by c_i_id
     * </pre>
     */
    Stream<CostMaster> selectOrderIid();

    /**
     * <pre>
     * update from cost_master set
     * c_stock_quantity = c_stock_quantity + :quantity,
     * c_stock_amount   = c_stock_amount   + :amount
     * where c_f_id = entity.cFId and c_i_id = entity.cIId
     * </pre>
     */
    int updateIncrease(CostMaster entity, BigDecimal quantity, BigDecimal amount);

    /**
     * <pre>
     * update from cost_master set
     * c_stock_quantity = c_stock_quantity + :quantity,
     * c_stock_amount   = c_stock_amount   - c_stock_amount * :quantity / c_stock_quantity
     * where c_f_id = entity.cFId and c_i_id = entity.cIId
     * </pre>
     */
    int updateDecrease(CostMaster entity, BigDecimal quantity);

    void forEach(Consumer<CostMaster> entityConsumer);
}
