package com.tsurugidb.benchmark.costaccounting.db.doma2.dao;

import java.time.LocalDate;
import java.util.List;

import org.seasar.doma.Dao;
import org.seasar.doma.Delete;
import org.seasar.doma.Insert;
import org.seasar.doma.Select;
import org.seasar.doma.Sql;

import com.tsurugidb.benchmark.costaccounting.db.doma2.config.AppConfig;
import com.tsurugidb.benchmark.costaccounting.db.doma2.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.SqlBetween;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.iceaxe.statement.TgVariable;

@Dao(config = AppConfig.class)
public interface ItemMasterDao {

    public static final String TABLE_NAME = "item_master";

    public static final String PS_COND_DATE = "? between i_effective_date and i_expired_date";
    static final String COND_DATE = "/* date */'2020-09-23' between i_effective_date and i_expired_date";

    public static final TgVariable<LocalDate> vDate = BenchVariable.ofDate("date");
    public static final String TG_COND_DATE = new SqlBetween(vDate, "i_effective_date", "i_expired_date").toString();

    @Delete
//	@Sql("delete from " + TABLE_NAME)
    @Sql("truncate table " + TABLE_NAME)
    int deleteAll();

    @Insert
    int insert(ItemMaster entity);

    @Select
    @Sql("select * from " + TABLE_NAME + " where i_id in /* ids */(1,2,3) and " + COND_DATE)
    List<ItemMaster> selectByIds(Iterable<Integer> ids, LocalDate date);

    @Select
    @Sql("select i_id from " + TABLE_NAME + " where " + COND_DATE + " and i_type = /* type */'product'")
    List<Integer> selectIdByType(LocalDate date, ItemType type);

    @Select
    @Sql("select * from " + TABLE_NAME)
    List<ItemMaster> selectAll();

    @Select
    @Sql("select max(i_id) + 1 from " + TABLE_NAME)
    Integer selectMaxId();

    @Select
    @Sql("select * from " + TABLE_NAME + " where i_id = /* id */1 and " + COND_DATE)
    ItemMaster selectById(int id, LocalDate date);
}
