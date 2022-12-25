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
import com.tsurugidb.benchmark.costaccounting.db.entity.HasDateRange;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.TsubakuroDao;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.benchmark.costaccounting.util.BenchRandom;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

public class InitialData {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) throws Exception {
        InitialData00CreateTable.main(args);
        InitialData01MeasurementMaster.main(args);
        InitialData02FactoryMaster.main(args);
        InitialData03ItemMaster.main(args);
        InitialData04ItemManufacturingMaster.main(args);
        InitialData05CostMaster.main(args);
        InitialData06ResultTable.main(args);
    }

    public static final LocalDate DEFAULT_BATCH_DATE = BenchConst.initBatchDate();

    protected CostBenchDbManager dbManager;

    protected final LocalDate batchDate;

    private LocalDateTime startTime;

    protected final BenchRandom random = new BenchRandom();

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
        int type = BenchConst.initDbManagerType();
        boolean isMultiSession = BenchConst.initDbManagerMultiSession();
        return CostBenchDbManager.createInstance(type, IsolationLevel.READ_COMMITTED, isMultiSession);
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
            setting.getTransactionOptionSupplier().setStateListener((attempt, e, state) -> {
                if (attempt > 0 && state.isExecute()) {
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
}
