/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.costaccounting.db.dao;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;

/**
 * 原価マスターDAO
 */
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

    int[] insertBatch(Collection<CostMaster> entityList);

    /**
     * <pre>
     * select * from cost_master
     * </pre>
     */
    Stream<CostMaster> selectAll();

    /**
     * <pre>
     * select * from cost_master
     * where c_f_id = :fId
     * </pre>
     */
    Stream<CostMaster> selectByFactory(int fId);

    /**
     * <pre>
     * select c_i_id from cost_master
     * where c_f_id = :fId
     * order by c_i_id
     * </pre>
     */
    List<Integer> selectIdByFactory(int fId);

    /**
     * <pre>
     * select * from cost_master
     * where c_f_id = :fId and c_i_id = :iId
     * </pre>
     */
    CostMaster selectById(int fId, int iId, boolean forUpdate);

    /**
     * <pre>
     * update cost_master set
     * c_stock_quantity = c_stock_quantity + :quantity,
     * c_stock_amount   = c_stock_amount   + :amount
     * where c_f_id = entity.cFId and c_i_id = entity.cIId
     * </pre>
     */
    int updateIncrease(CostMaster entity, BigDecimal quantity, BigDecimal amount);

    /**
     * <pre>
     * update cost_master set
     * c_stock_quantity = c_stock_quantity + :quantity,
     * c_stock_amount   = c_stock_amount   - c_stock_amount * :quantity / c_stock_quantity
     * where c_f_id = entity.cFId and c_i_id = entity.cIId
     * </pre>
     */
    int updateDecrease(CostMaster entity, BigDecimal quantity);

    /**
     * <pre>
     * update cost_master set
     * c_stock_quantity = 0,
     * c_stock_amount   = 0
     * where c_f_id = entity.cFId and c_i_id = entity.cIId
     * </pre>
     */
    int updateZero(CostMaster entity);

    void forEach(Consumer<CostMaster> entityConsumer);
}
