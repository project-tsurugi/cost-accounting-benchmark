package com.tsurugidb.benchmark.costaccounting.db.doma2.dao;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Stream;

import org.seasar.doma.Dao;
import org.seasar.doma.Delete;
import org.seasar.doma.Insert;
import org.seasar.doma.Select;
import org.seasar.doma.Sql;
import org.seasar.doma.Suppress;
import org.seasar.doma.Update;
import org.seasar.doma.jdbc.SelectOptions;
import org.seasar.doma.message.Message;

import com.tsurugidb.benchmark.costaccounting.db.doma2.config.AppConfig;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemManufacturingMasterIds;
import com.tsurugidb.iceaxe.statement.TgVariable;

@Dao(config = AppConfig.class)
public interface ItemManufacturingMasterDao {

    public static final String TABLE_NAME = "item_manufacturing_master";

    public static final String PS_COND_DATE = "? between im_effective_date and im_expired_date";
    static final String COND_DATE = "/* date */'2020-09-24' between im_effective_date and im_expired_date";

    public static final TgVariable<LocalDate> vDate = TgVariable.ofDate("date");
    static final String TG_COND_DATE = vDate.sqlName() + " between im_effective_date and im_expired_date";

    @Delete
//	@Sql("delete from " + TABLE_NAME)
    @Sql("truncate table " + TABLE_NAME)
    int deleteAll();

    @Insert
    int insert(ItemManufacturingMaster entity);

    @Select
    @Sql("select * from " + TABLE_NAME + " where " + COND_DATE + " order by im_f_id, im_i_id")
    List<ItemManufacturingMaster> selectAll(LocalDate date);

    @Select
    @Sql("select * from " + TABLE_NAME)
    List<ItemManufacturingMaster> selectAll();

    @Select
    @Sql("select im_f_id, im_i_id from " + TABLE_NAME + " where " + COND_DATE + " order by im_f_id, im_i_id")
    List<ItemManufacturingMasterIds> selectIds(LocalDate date);

    @Select
    @Sql("select * from " + TABLE_NAME + " where im_f_id = /* factoryId */1 and " + COND_DATE)
    @Suppress(messages = { Message.DOMA4274 })
    Stream<ItemManufacturingMaster> selectByFactory(int factoryId, LocalDate date);

    @Select
    @Sql("select * from " + TABLE_NAME + " where im_f_id in /* factoryIdList */(1,2,3) and " + COND_DATE)
    @Suppress(messages = { Message.DOMA4274 })
    Stream<ItemManufacturingMaster> selectByFactories(List<Integer> factoryIdList, LocalDate date);

    @Select
    @Sql("select * from " + TABLE_NAME + " where im_f_id = /* factoryId */1 and im_i_id = /* itemId */2 and " + COND_DATE)
    ItemManufacturingMaster selectById(int factoryId, int itemId, LocalDate date);

    @Select
    @Sql("select * from " + TABLE_NAME + " where im_f_id = /* factoryId */1 and im_i_id = /* itemId */2 and " + COND_DATE)
    ItemManufacturingMaster selectByIdOptions(int factoryId, int itemId, LocalDate date, SelectOptions options);

    default ItemManufacturingMaster selectByIdForUpdate(int factoryId, int itemId, LocalDate date) {
        SelectOptions options = SelectOptions.get().forUpdate();
        return selectByIdOptions(factoryId, itemId, date, options);
    }

    @Select
    @Sql("select * from " + TABLE_NAME + " where im_f_id = /* factoryId */1 and im_i_id = /* itemId */2 and " + "/* date */'2020-09-24' < im_effective_date" //
            + " order by im_effective_date")
    List<ItemManufacturingMaster> selectByIdFuture(int factoryId, int itemId, LocalDate date);

    @Update
    int update(ItemManufacturingMaster entity);

    @Select
    @Sql("select im_i_id from " + TABLE_NAME + " where im_f_id = /* factoryId */1 and " + COND_DATE)
    List<Integer> selectIdByFactory(int factoryId, LocalDate date);
}
