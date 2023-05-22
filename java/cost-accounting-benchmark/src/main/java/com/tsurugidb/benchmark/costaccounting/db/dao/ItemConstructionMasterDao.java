package com.tsurugidb.benchmark.costaccounting.db.dao;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMasterIds;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMasterKey;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.SqlBetween;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;

public interface ItemConstructionMasterDao {

    public static final String TABLE_NAME = "item_construction_master";

    public static final String PS_COND_DATE = "? between ic_effective_date and ic_expired_date";

    public static final TgBindVariable<LocalDate> vDate = BenchVariable.ofDate("date");
    public static final String TG_COND_DATE = new SqlBetween(vDate, "ic_effective_date", "ic_expired_date").toString();

    /**
     * <pre>
     * truncate table item_construction_master
     * </pre>
     */
    void truncate();

    /**
     * <pre>
     * delete from item_construction_master
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * insert into item_construction_master
     * values(:entity)
     * </pre>
     */
    int insert(ItemConstructionMaster entity);

    int[] insertBatch(Collection<ItemConstructionMaster> entityList);

    /**
     * <pre>
     * select * from item_construction_master
     * where :date between ic_effective_date and ic_expired_date
     * order by ic_parent_i_id, ic_i_id
     * </pre>
     */
    List<ItemConstructionMaster> selectAll(LocalDate date);

    /**
     * <pre>
     * select * from item_construction_master
     * </pre>
     */
    List<ItemConstructionMaster> selectAll();

    /**
     * <pre>
     * select ic_parent_i_id, ic_i_id from item_construction_master
     * where :date between ic_effective_date and ic_expired_date
     * order by ic_parent_i_id, ic_i_id
     * </pre>
     */
    List<ItemConstructionMasterIds> selectIds(LocalDate date);

    /**
     * <pre>
     * select * from item_construction_master
     * where ic_parent_i_id = :parentId
     *   and :date between ic_effective_date and ic_expired_date
     * order by ic_i_id
     * </pre>
     */
    List<ItemConstructionMaster> selectByParentId(int parentId, LocalDate date);

    /**
     * <pre>
     * select * from item_construction_master
     * where ic_parent_i_id = :parentId
     *   and ic_i_id = :itemId
     *   and :date between ic_effective_date and ic_expired_date
     * </pre>
     */
    ItemConstructionMaster selectById(int parentId, int itemId, LocalDate date);

    /**
     * <pre>
     * with recursive
     * ic as (
     *   select * from item_construction_master
     *   where :date between ic_effective_date and ic_expired_date
     * ),
     * r as (
     *   select * from ic
     *   where ic_parent_i_id = :parentId
     *   union all
     *   select ic.* from ic, r
     *   where ic.ic_parent_i_id = r.ic_i_id
     * )
     * select * from r
     * order by ic_parent_i_id, ic_i_id
     * </pre>
     */
    Stream<ItemConstructionMaster> selectRecursiveByParentId(int parentId, LocalDate date);

    /**
     * <pre>
     * select ic_parent_i_id, ic_i_id, ic_effective_date
     * from item_construction_master ic
     * left join item_master i on i_id=ic_i_id and :date between i_effective_date and i_expired_date
     * where :date between ic_effective_date and ic_expired_date
     *   and i_type in (:typeList)
     * </pre>
     */
    List<ItemConstructionMasterKey> selectByItemType(LocalDate date, List<ItemType> typeList);

    /**
     * <pre>
     * select * from item_construction_master
     * where ic_i_id = :in.icIId
     *   and ic_parent_i_id = :in.icParentIId
     *   and ic_effective_date = :in.icEffectiveDate
     * for update
     * </pre>
     */
    ItemConstructionMaster lock(ItemConstructionMaster in);

    /**
     * <pre>
     * delete item_construction_master
     * where ic_parent_i_id = :key.ic_parent_i_id
     *   and ic_i_id = :key.ic_i_id
     *   and ic_effective_date = :key.ic_effective_date
     * </pre>
     */
    int delete(ItemConstructionMasterKey key);

    void forEach(Consumer<ItemConstructionMaster> entityConsumer);
}
