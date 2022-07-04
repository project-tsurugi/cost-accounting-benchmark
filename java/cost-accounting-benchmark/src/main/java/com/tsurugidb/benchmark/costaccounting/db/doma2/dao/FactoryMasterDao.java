package com.tsurugidb.benchmark.costaccounting.db.doma2.dao;

import java.util.List;

import org.seasar.doma.Dao;
import org.seasar.doma.Delete;
import org.seasar.doma.Insert;
import org.seasar.doma.Select;
import org.seasar.doma.Sql;

import com.tsurugidb.benchmark.costaccounting.db.doma2.config.AppConfig;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.FactoryMaster;

@Dao(config = AppConfig.class)
public interface FactoryMasterDao {

    public static final String TABLE_NAME = "factory_master";

    @Delete
//	@Sql("delete from " + TABLE_NAME)
    @Sql("truncate table " + TABLE_NAME)
    int deleteAll();

    @Insert
    int insert(FactoryMaster entity);

    @Select
    @Sql("select f_id from " + TABLE_NAME)
    List<Integer> selectAllId();

    @Select
    @Sql("select * from " + TABLE_NAME + " where f_id = /* factoryId */1")
    FactoryMaster selectById(int factoryId);
}
