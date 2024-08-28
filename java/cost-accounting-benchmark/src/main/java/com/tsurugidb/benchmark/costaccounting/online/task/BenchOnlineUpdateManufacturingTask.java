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
package com.tsurugidb.benchmark.costaccounting.online.task;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.UniqueConstraintException;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.init.InitialData04ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.event.TsurugiTransactionEventListener;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 生産数の変更
 */
public class BenchOnlineUpdateManufacturingTask extends BenchOnlineTask {
    public static final String TASK_NAME = "update-manufacturing";

    private static RandomKeySelector<Integer> itemMasterProductKeySelector;

    public static void clearPrepareData() {
        itemMasterProductKeySelector = null;
    }

    private TgTmSetting settingMain;
    private double coverRate;

    public BenchOnlineUpdateManufacturingTask(int taskId) {
        super(TASK_NAME, taskId);
    }

    @Override
    public void initializeSetting() {
        this.settingMain = config.getSetting(LOG, this, () -> {
            if (BenchConst.useReadArea()) {
                return TgTxOption.ofLTX(BenchConst.DEFAULT_TX_OPTION).addWritePreserve(ItemManufacturingMasterDao.TABLE_NAME) //
                        .addInclusiveReadArea(ItemManufacturingMasterDao.TABLE_NAME);
            } else {
                return TgTxOption.ofLTX(ItemManufacturingMasterDao.TABLE_NAME);
            }
        });
        setTxOptionDescription(settingMain);
        this.coverRate = config.getCoverRateForTask(title);
    }

    @Override
    public void executePrepare() {
        var setting = TgTmSetting.of(TgTxOption.ofRTX().label(TASK_NAME + ".prepare"));
        var date = config.getBatchDate();

        cacheItemMasterProductKeyList(setting, date);
    }

    private long commitOccTime = 0, commitLtxTime = 0, commitFailTime = 0, rollbackTime = 0;
    private int commitOccCount = 0, commitLtxCount = 0, commitFailCount = 0, rollbackCount = 0;

    @Override
    protected boolean execute1() {
        int productId = selectRandomItemId();
        if (productId < 0) {
            return false;
        }
        logTarget("factory=%d, date=%s, product=%d", factoryId, date, productId);

        int newQuantity = random.random(0, 500) * 100;

        for (;;) {
            try {
                boolean ok = dbManager.execute(settingMain, () -> {
                    if (dbManager instanceof CostBenchDbManagerIceaxe) {
                        var m = (CostBenchDbManagerIceaxe) dbManager;
                        var transaction = m.getCurrentTransaction();
                        transaction.addEventListener(iceaxeFinishCounter);
                    }
                    return executeMain(productId, newQuantity);
                });
                if (ok) {
                    return true;
                }
            } catch (UniqueConstraintException e) {
                LOG.debug("duplicate item_manufacturing_master (transaction)", e);
                continue;
            }
        }
    }

    private final TsurugiTransactionEventListener iceaxeFinishCounter = new TsurugiTransactionEventListener() {
        private long commitStart, rollbackStart;

        @Override
        public void commitStart(TsurugiTransaction transaction, TgCommitType commitType) {
            this.commitStart = System.nanoTime();
        }

        @Override
        public void commitEnd(TsurugiTransaction transaction, TgCommitType commitType, Throwable occurred) {
            if (occurred == null) {
                long time = System.nanoTime() - commitStart;
                if (transaction.getTransactionOption().isOCC()) {
                    commitOccTime += time;
                    commitOccCount++;
                } else {
                    commitLtxTime += time;
                    commitLtxCount++;
                }
            } else {
                commitFailTime += System.nanoTime() - commitStart;
                commitFailTime++;
            }
        }

        @Override
        public void rollbackStart(TsurugiTransaction transaction) {
            this.rollbackStart = System.nanoTime();
        }

        @Override
        public void rollbackEnd(TsurugiTransaction transaction, Throwable occurred) {
            rollbackTime += System.nanoTime() - rollbackStart;
            rollbackCount++;
        }
    };

    private long randomTime = 0;
    private int randomCount = 0;

    protected int selectRandomItemId() {
        long start = System.nanoTime();
        RandomKeySelector<Integer> selector;
        if (itemMasterProductKeySelector != null) {
            selector = itemMasterProductKeySelector;
        } else {
            var list = selectItemMasterProductKeyList(date);
            if (list.isEmpty()) {
                return -1;
            }
            selector = new RandomKeySelector<>(list, random.getRawRandom(), 0, coverRate);
        }
        int itemId = selector.get();
        this.randomTime += System.nanoTime() - start;
        this.randomCount++;
        return itemId;
    }

    private void cacheItemMasterProductKeyList(TgTmSetting setting, LocalDate date) {
        synchronized (BenchOnlineUpdateManufacturingTask.class) {
            if (itemMasterProductKeySelector == null) {
                var log = LoggerFactory.getLogger(BenchOnlineUpdateManufacturingTask.class);
                dbManager.execute(setting, () -> {
                    log.info("itemMasterDao.selectIdByType(PRODUCT) start");
                    var list = selectItemMasterProductKeyList(date);
                    log.info("itemMasterDao.selectIdByType(PRODUCT) end. size={}", list.size());
                    itemMasterProductKeySelector = new RandomKeySelector<>(list, random.getRawRandom(), 0, coverRate);
                });
            }
        }
    }

    private List<Integer> selectItemMasterProductKeyList(LocalDate date) {
        var dao = dbManager.getItemMasterDao();
        return dao.selectIdByType(date, ItemType.PRODUCT);
    }

    protected boolean executeMain(int productId, int newQuantity) {
        for (;;) {
            boolean ok = executeMain1(productId, newQuantity);
            if (ok) {
                return true;
            }

            switch (1) {
            case 1:
                // 別コネクションでリトライ
                return false;
            default:
                // 同一コネクション内でリトライ（PostgreSQLだと例外発生時に同一コネクションでSQLを発行するとエラーになる）
                continue;
            }
        }
    }

    private long selectIdTime = 0, selectFutureTime = 0;
    private int selectIdCount = 0, selectFutureCount = 0;
    private long insertTime = 0, insertDupTime = 0, insertExceptionTime = 0;
    private int insertCount = 0, insertDupCount = 0, insertExceptionCount = 0;
    private long updateTime = 0, updateExceptionTime = 0;
    private int updateCount = 0, updateExceptionCount = 0;

    protected boolean executeMain1(int productId, int newQuantity) {
        long selectIdStart = System.nanoTime();
        ItemManufacturingMasterDao itemManufacturingMasterDao = dbManager.getItemManufacturingMasterDao();
        ItemManufacturingMaster entity = itemManufacturingMasterDao.selectByIdForUpdate(factoryId, productId, date);
        this.selectIdTime = System.nanoTime() - selectIdStart;
        this.selectIdCount++;
        if (entity == null) {
            long start = System.nanoTime();
            try {
                InitialData04ItemManufacturingMaster initialData = new InitialData04ItemManufacturingMaster(1, date);
                entity = initialData.newItemManufacturingMaster(factoryId, productId);
                entity.setEffectiveDate(date);
                {
                    long selectFutureStart = System.nanoTime();
                    List<ItemManufacturingMaster> list = itemManufacturingMasterDao.selectByIdFuture(productId, productId, date);
                    this.selectFutureTime += System.nanoTime() - selectFutureStart;
                    this.selectFutureCount++;
                    start = System.nanoTime();
                    if (!list.isEmpty()) {
                        ItemManufacturingMaster min = list.get(0);
                        entity.setExpiredDate(min.getEffectiveDate().minusDays(1));
                    } else {
                        entity.setExpiredDate(LocalDate.of(9999, 12, 31));
                    }
                }
                entity.setImManufacturingQuantity(BigInteger.valueOf(newQuantity));
                long insertStart = System.nanoTime();
                try {
                    itemManufacturingMasterDao.insert(entity);
                } catch (UniqueConstraintException e) {
                    LOG.debug("duplicate item_manufacturing_master (insert)", e);
                    this.insertDupTime += System.nanoTime() - insertStart;
                    this.insertDupCount++;
                    return false;
                }
                this.insertTime += System.nanoTime() - insertStart;
                this.insertCount++;
            } catch (Throwable e) {
                this.insertExceptionTime += System.nanoTime() - start;
                this.insertExceptionCount++;
                throw e;
            }
        } else {
            long start = System.nanoTime();
            try {
                entity.setImManufacturingQuantity(BigInteger.valueOf(newQuantity));
                itemManufacturingMasterDao.update(entity);
                this.updateTime += System.nanoTime() - start;
                this.updateCount++;
            } catch (Throwable e) {
                this.updateExceptionTime += System.nanoTime() - start;
                this.updateExceptionCount++;
                throw e;
            }
        }

        return true;
    }

    @Override
    public void close() {
        dumpTime("random", randomTime, randomCount);
        dumpTime("selectId", selectIdTime, selectIdCount);
        dumpTime("selectFuture", selectFutureTime, selectFutureCount);
        dumpTime("insert", insertTime, insertCount);
        dumpTime("insert.dup", insertDupTime, insertDupCount);
        dumpTime("insert.fail", insertExceptionTime, insertExceptionCount);
        dumpTime("update", updateTime, updateCount);
        dumpTime("update.fail", updateExceptionTime, updateExceptionCount);
        dumpTime("commit.occ", commitOccTime, commitOccCount);
        dumpTime("commit.ltx", commitLtxTime, commitLtxCount);
        dumpTime("commit.fail", commitFailTime, commitFailCount);
        dumpTime("rollback", rollbackTime, rollbackCount);
    }

    private void dumpTime(String title, long time, int count) {
        LOG.info(title + " time={}[ms], count={}, avg={}[ms]", String.format("%,.3f", time / 1000_000d), count, String.format("%,.3f", (double) time / count / 1000_000d));
    }

    // for test
    public static void main(String[] args) {
        try (BenchOnlineUpdateManufacturingTask task = new BenchOnlineUpdateManufacturingTask(0)) {
            try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
                task.setDao(null, manager);

                task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

                task.execute();
//              manager.execute(() -> {
//                  task.executeMain(49666);
//              });
            }
        }
    }
}
