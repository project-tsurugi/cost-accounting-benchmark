package com.example.nedo.db.doma2.dao;

import java.util.List;

import org.seasar.doma.Dao;
import org.seasar.doma.Delete;
import org.seasar.doma.Insert;
import org.seasar.doma.Select;
import org.seasar.doma.Sql;

import com.example.nedo.db.doma2.config.AppConfig;
import com.example.nedo.db.doma2.entity.MeasurementMaster;

@Dao(config = AppConfig.class)
public interface MeasurementMasterDao {

    public static final String TABLE_NAME = "measurement_master";

    @Delete
//	@Sql("delete from " + TABLE_NAME)
    @Sql("truncate table " + TABLE_NAME)
    int deleteAll();

    @Insert
    int insert(MeasurementMaster entity);

    @Select
    @Sql("select * from " + TABLE_NAME)
    public List<MeasurementMaster> selectAll();
}
