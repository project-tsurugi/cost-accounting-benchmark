package com.tsurugidb.benchmark.costaccounting.db.dao;

import java.time.LocalDate;
import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.SqlBetween;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.statement.TgVariable;

public interface ItemMasterDao {

    public static final String TABLE_NAME = "item_master";

    public static final String PS_COND_DATE = "? between i_effective_date and i_expired_date";

    public static final TgVariable<LocalDate> vDate = BenchVariable.ofDate("date");
    public static final String TG_COND_DATE = new SqlBetween(vDate, "i_effective_date", "i_expired_date").toString();

    /**
     * <pre>
     * delete from item_master
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * insert into item_master
     * values(:entity)
     * </pre>
     */
    int insert(ItemMaster entity);

    /**
     * <pre>
     * select * from item_master
     * where i_id in (:ids)
     *   and :date between i_effective_date and i_expired_date
     * </pre>
     */
    List<ItemMaster> selectByIds(Iterable<Integer> ids, LocalDate date);

    /**
     * <pre>
     * select i_id from item_master
     * where :date between i_effective_date and i_expired_date
     *   and i_type = :type
     * </pre>
     */
    List<Integer> selectIdByType(LocalDate date, ItemType type);

    /**
     * <pre>
     * select * from item_master
     * </pre>
     */
    List<ItemMaster> selectAll();

    /**
     * <pre>
     * select max(i_id) + 1 from item_master
     * </pre>
     */
    Integer selectMaxId();

    /**
     * <pre>
     * select * from item_master
     * where i_id = :id
     *   and :date between i_effective_date and i_expired_date
     * </pre>
     */
    ItemMaster selectById(int id, LocalDate date);
}