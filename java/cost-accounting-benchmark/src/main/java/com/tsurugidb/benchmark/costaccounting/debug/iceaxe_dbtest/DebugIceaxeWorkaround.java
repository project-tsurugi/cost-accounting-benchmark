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
package com.tsurugidb.benchmark.costaccounting.debug.iceaxe_dbtest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager.DbManagerPurpose;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMasterKey;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ItemMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.SqlIn;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain.BenchVariable;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.DbManagerType;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Workaround test
 */
// SQLのWORKAROUNDを除去したものが、WORKAROUND時の結果と同じであることを確認する
public class DebugIceaxeWorkaround extends DbTester {
    private static final Logger LOG = LoggerFactory.getLogger(DebugIceaxeWorkaround.class);

    private final LocalDate batchDate = BenchConst.initBatchDate();

    public DebugIceaxeWorkaround(String[] args) {
    }

    public void execute() throws IOException, InterruptedException {
        try (var manager = CostBenchDbManager.createInstance(DbManagerType.ICEAXE, DbManagerPurpose.DEBUG, IsolationLevel.SERIALIZABLE, true)) {
            test_ItemMasterDao_selectByIds(manager);
            test_ItemConstructionMasterDao_selectByItemType(manager);
            test_ResultTableDao_selectRequiredQuantity(manager);
            LOG.info("end");
        } finally {
            closeSession();
        }
    }

    void test_ItemMasterDao_selectByIds(CostBenchDbManager manager) throws IOException, InterruptedException {
        LOG.info("ItemMasterDao.selectByIds()");

        var idSet = new HashSet<Integer>();
        {
            var entity = getMaxMaterialId();
            int minId = entity.getInt(0);
            int maxId = entity.getInt(1);

            long seed = 1;
            var random = new Random(seed);
            for (int i = 0; i < 20; i++) {
                int id = random.nextInt(maxId - minId + 1) + minId;
                idSet.add(id);
            }
        }

        var dao = manager.getItemMasterDao();

        var setting = TgTmSetting.of(TgTxOption.ofOCC());
        manager.execute(setting, () -> {
            List<ItemMaster> actualList = dao.selectByIds(idSet, batchDate);

            var expectedList = new ArrayList<ItemMaster>(idSet.size());
            for (var id : idSet) {
                ItemMaster entity = dao.selectById(id, batchDate);
                expectedList.add(entity);
            }
            expectedList.sort(Comparator.comparing(ItemMaster::getIId));

            LOG.info("expectedList.size={}, actualList.size={}", expectedList.size(), actualList.size());
            if (expectedList.size() != actualList.size()) {
                LOG.error("ItemMasterDao.selectByIds() error.\nexpected={}\nactual={}", expectedList, actualList);
                throw new RuntimeException("ItemMasterDao.selectByIds() error");
            }
            for (int i = 0; i < expectedList.size(); i++) {
                ItemMaster expected = expectedList.get(i);
                ItemMaster actual = actualList.get(i);
//              if (!expected.getIId().equals(actual.getIId())) {
                if (!expected.toString().equals(actual.toString())) {
                    LOG.error("ItemMasterDao.selectByIds() error.\nexpected={}\nactual={}", expected, actual);
                    throw new RuntimeException("ItemMasterDao.selectByIds() error");
                }
            }
        });
    }

    private TsurugiResultEntity getMaxMaterialId() throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        return tm.executeAndFindRecord("select min(i_id), max(i_id) from item_master where i_type='raw_material'").get();
    }

    void test_ItemConstructionMasterDao_selectByItemType(CostBenchDbManager manager) throws IOException {
        var dao = manager.getItemConstructionMasterDao();

        var setting = TgTmSetting.of(TgTxOption.ofOCC());
        for (var typeList : List.of(List.of(ItemType.PRODUCT, ItemType.WORK_IN_PROCESS), List.of(ItemType.RAW_MATERIAL))) {
            LOG.info("ItemConstructionMasterDao.selectByItemType() typeList={}", typeList);

            List<ItemConstructionMasterKey> actualList = manager.execute(setting, () -> {
                return dao.selectByItemType(batchDate, typeList);
            });

            List<ItemConstructionMasterKey> expectedList = expected_selectByItemType(typeList);

            LOG.info("expectedList.size={}, actualList.size={}", expectedList.size(), actualList.size());
            if (expectedList.size() != actualList.size()) {
                LOG.error("ItemConstructionMasterDao.selectByItemType() error.\nexpected={}\nactual={}", expectedList, actualList);
                throw new RuntimeException("ItemConstructionMasterDao.selectByItemType()() error");
            }
            for (int i = 0; i < expectedList.size(); i++) {
                ItemConstructionMasterKey expected = expectedList.get(i);
                ItemConstructionMasterKey actual = actualList.get(i);
                if (!expected.equals(actual)) {
                    LOG.error("ItemConstructionMasterDao.selectByItemType()() error.\nexpected={}\nactual={}", expected, actual);
                    throw new RuntimeException("ItemConstructionMasterDao.selectByItemType()() error");
                }
            }
        }
    }

    private List<ItemConstructionMasterKey> expected_selectByItemType(List<ItemType> typeList) throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);

        var vDate = BenchVariable.ofDate("date");
        var variables = TgBindVariables.of();
        variables.add(vDate);
        var parameter = TgBindParameters.of();
        parameter.add(vDate.bind(batchDate));

        var inSql = new SqlIn("i_type");
        int i = 0;
        for (var type : typeList) {
            var variable = BenchVariable.ofItemType("t" + (i++));
            variables.add(variable);
            inSql.add(variable);
            parameter.add(variable.bind(type));
        }

        var sql = "select ic_parent_i_id, ic_i_id, ic_effective_date" //
                + " from " + ItemConstructionMasterDao.TABLE_NAME + " ic" //
                + " inner join " + ItemMasterDao.TABLE_NAME + " i on i_id=ic_i_id and " + ItemMasterDaoIceaxe.TG_COND_DATE //
                + " where " + ItemConstructionMasterDao.TG_COND_DATE //
                + " and " + inSql //
        ;
        var parameterMapping = TgParameterMapping.of(variables);
        var resultMapping = TgResultMapping.of(ItemConstructionMasterKey::new) //
                .addInt(ItemConstructionMasterKey::setIcParentIId) //
                .addInt(ItemConstructionMasterKey::setIcIId) //
                .addDate(ItemConstructionMasterKey::setIcEffectiveDate);
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            return tm.executeAndGetList(ps, parameter);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    void test_ResultTableDao_selectRequiredQuantity(CostBenchDbManager manager) throws IOException, InterruptedException {
        var dao = manager.getResultTableDao();

        for (int fId = 1; fId <= 4; fId++) {
            final int factoryId = fId;
            LOG.info("ResultTableDao.selectRequiredQuantity() factoryId={}", factoryId);

            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            List<ResultTable> actualList = manager.execute(setting, () -> {
                Stream<ResultTable> actualStream = dao.selectRequiredQuantity(factoryId, batchDate);
                return actualStream.collect(Collectors.toList());
            });

            List<ResultTable> expectedList = expected_selectRequiredQuantity(factoryId);

            LOG.info("expectedList.size={}, actualList.size={}", expectedList.size(), actualList.size());
            if (expectedList.size() != actualList.size()) {
                LOG.error("ResultTableDao.selectRequiredQuantity() error.\nexpected={}\nactual={}", expectedList, actualList);
                throw new RuntimeException("ResultTableDao.selectRequiredQuantity() error");
            }
            for (int i = 0; i < expectedList.size(); i++) {
                ResultTable expected = expectedList.get(i);
                ResultTable actual = actualList.get(i);
                if (!expected.toString().equals(actual.toString())) {
                    LOG.error("ResultTableDao.selectRequiredQuantity() error.\nexpected={}\nactual={}", expected, actual);
                    throw new RuntimeException("ResultTableDao.selectRequiredQuantity() error");
                }
            }
        }
    }

    private List<ResultTable> expected_selectRequiredQuantity(int factoryId) throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var sql = "select" //
                + "  r_f_id," //
                + "  r_manufacturing_date," //
                + "  r_i_id," //
                + "  sum(r_required_quantity) as r_required_quantity," //
                + "  max(r_required_quantity_unit) as r_required_quantity_unit" //
                + " from result_table r" //
                + " inner join item_master m on m.i_id=r.r_i_id and m.i_effective_date<=r.r_manufacturing_date and r.r_manufacturing_date<=m.i_expired_date" //
                + " where r_f_id=" + factoryId + " and r_manufacturing_date=date'" + batchDate + "' and m.i_type='raw_material'" //
                + " group by r_f_id, r_manufacturing_date, r_i_id" //
        // + " order by r_i_id"
        ;
        var mapping = TgResultMapping.of(ResultTable::new) //
                .addInt(ResultTable::setRFId) //
                .addDate(ResultTable::setRManufacturingDate) //
                .addInt(ResultTable::setRIId) //
                .addDecimal(ResultTable::setRRequiredQuantity) //
                .addString(ResultTable::setRRequiredQuantityUnit) //
        ;
        List<ResultTable> list = tm.executeAndGetList(sql, mapping);
        list.sort(Comparator.comparing(ResultTable::getRIId));
        return list;
    }
}
