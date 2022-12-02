package com.tsurugidb.benchmark.costaccounting.db.dao;

import java.util.List;
import java.util.function.Consumer;

import com.tsurugidb.benchmark.costaccounting.db.entity.MeasurementMaster;

public interface MeasurementMasterDao {

    public static final String TABLE_NAME = "measurement_master";

    /**
     * <pre>
     * delete from measurement_master
     * </pre>
     */
    int deleteAll();

    /**
     * <pre>
     * insert into measurement_master
     * values(:entity)
     * </pre>
     */
    int insert(MeasurementMaster entity);

    /**
     * <pre>
     * select * from measurement_master
     * </pre>
     */
    List<MeasurementMaster> selectAll();

    void forEach(Consumer<MeasurementMaster> entityConsumer);
}
