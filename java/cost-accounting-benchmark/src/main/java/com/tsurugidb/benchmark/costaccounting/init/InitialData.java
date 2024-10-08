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
package com.tsurugidb.benchmark.costaccounting.init;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager.DbManagerPurpose;
import com.tsurugidb.benchmark.costaccounting.db.entity.HasDateRange;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.TsubakuroDao;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.benchmark.costaccounting.util.BenchReproducibleRandom;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class InitialData {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    public static void main(String... args) throws Exception {
        InitialData00CreateTable.main(args);
        InitialData01MeasurementMaster.main(args);
        InitialData02FactoryMaster.main(args);
        InitialData03ItemMaster.main(args);
        InitialData04ItemManufacturingMaster.main(args);
        InitialData05CostMaster.main(args);
        InitialData06StockHistory.main(args);
        InitialData07ResultTable.main(args);
    }

    public static final LocalDate DEFAULT_BATCH_DATE = BenchConst.initBatchDate();

    protected CostBenchDbManager dbManager;

    protected final LocalDate batchDate;

    private LocalDateTime startTime;

    protected final BenchReproducibleRandom random = new BenchReproducibleRandom();

    protected InitialData(LocalDate batchDate) {
        this.batchDate = batchDate;
    }

    protected CostBenchDbManager initializeDbManager() {
        if (dbManager == null) {
            this.dbManager = createDbManager();
        }
        return dbManager;
    }

    static CostBenchDbManager createDbManager() {
        var type = BenchConst.initDbManagerType();
        boolean isMultiSession = BenchConst.initDbManagerMultiSession();
        return CostBenchDbManager.createInstance(type, DbManagerPurpose.INIT_DATA, IsolationLevel.READ_COMMITTED, isMultiSession);
    }

    protected void logStart() {
        this.startTime = LocalDateTime.now();
        LOG.info("start {}", startTime);
    }

    protected void logEnd() {
        LocalDateTime endTime = LocalDateTime.now();
        LOG.info("end {}[s]", startTime.until(endTime, ChronoUnit.SECONDS));
    }

    protected void initializeStartEndDate(int seed, HasDateRange entity) {
        LocalDate startDate = batchDate.minusDays(random(seed, 0, 700));
        entity.setEffectiveDate(startDate);

        LocalDate endDate = getRandomExpiredDate(seed + 1, batchDate);
        entity.setExpiredDate(endDate);
    }

    public LocalDate getRandomExpiredDate(int seed, LocalDate batchDate) {
        LocalDate endDate = batchDate.plusDays(random(seed, 7, 700));
        return endDate;
    }

    protected TgTmSetting getSetting(String... wp) {
        return getSetting(() -> TgTxOption.ofLTX(wp));
    }

    protected TgTmSetting getSetting(Supplier<TgTxOption> longTxSupplier) {
        String txText = BenchConst.initTsurugiTxOption();
        switch (txText.toUpperCase()) {
        case "OCC":
            var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
            setting.getTransactionOptionSupplier().setTmOptionListener((attempt, e, tmOption) -> {
                if (attempt > 0 && tmOption.isExecute()) {
                    LOG.info("OCC retry {} (exception={})", attempt, e.getMessage());
                }
            });
            return setting;
        default:
            return TgTmSetting.of(longTxSupplier.get());
        }
    }

    protected void dumpExplainCounter(Object dao) {
        if (dao instanceof TsubakuroDao) {
            ((TsubakuroDao<?>) dao).dumpExplainCounter();
        }
    }

    // random

    protected int random(int seed, int start, int end) {
        return random.prandom(seed, start, end);
    }

    protected BigDecimal random(int seed, BigDecimal start, BigDecimal end) {
        return random.prandom(seed, start, end);
    }

    public <T> T getRandomAndRemove(int seed, List<T> list) {
        assert list.size() > 0;

        int i = random.prandom(seed, list.size());
        return list.remove(i);
    }

    protected void randomShuffle(List<Integer> list) {
        randomShuffle(list, 0, list.size());
    }

    public void randomShuffle(List<Integer> list, int seed, int shuffleSize) {
        int listSize = list.size();
        for (int i = 0; i < shuffleSize; i++) {
            int j = random.prandom(seed + i + 1, listSize);
            Integer v1 = list.get(i);
            Integer v2 = list.get(j);
            list.set(i, v2);
            list.set(j, v1);
        }
    }

    // thread

    private ForkJoinPool forkJoinPool = null;
    private final List<ForkJoinTask<?>> taskList = new ArrayList<>();

    protected void executeTask(ForkJoinTask<?> task) {
        if (forkJoinPool == null) {
            int parallelism = BenchConst.initParallelism();
            LOG.info("ForkJoinPool.parallelism={}", parallelism);
            forkJoinPool = new ForkJoinPool(parallelism);
        }
        forkJoinPool.execute(task);
        taskList.add(task);
    }

    protected void joinAllTask() {
        for (ForkJoinTask<?> task : taskList) {
            task.join();
        }
        taskList.clear();
    }

    protected void shutdown() {
        if (forkJoinPool != null) {
            forkJoinPool.shutdownNow();
        }
    }
}
