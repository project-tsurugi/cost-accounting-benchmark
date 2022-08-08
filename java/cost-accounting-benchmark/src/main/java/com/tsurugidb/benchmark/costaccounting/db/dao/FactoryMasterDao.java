package com.tsurugidb.benchmark.costaccounting.db.dao;

import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.entity.FactoryMaster;

public interface FactoryMasterDao {

    public static final String TABLE_NAME = "factory_master";

    /**
     * <pre>
     * delete from factory_master
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * insert into factory_master
     * values(:entity)
     * </pre>
     */
    int insert(FactoryMaster entity);

    /**
     * <pre>
     * select f_id from factory_master
     * </pre>
     */
    List<Integer> selectAllId();

    /**
     * <pre>
     * select * from factory_master
     * where f_id = :factoryId
     * </pre>
     */
    FactoryMaster selectById(int factoryId);
}
