/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.benchmark.costaccounting.online.command;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.batch.BatchConfig;
import com.tsurugidb.benchmark.costaccounting.batch.CostAccountingBatch;
import com.tsurugidb.benchmark.costaccounting.batch.StringUtil;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager.DbManagerPurpose;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.online.CostAccountingOnline;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.benchmark.costaccounting.watcher.TsurugidbWatcher;
import com.tsurugidb.benchmark.costaccounting.watcher.TsurugidbWatcherService;

public class OnlineCbCommand implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineCbCommand.class);

    private final OnlineAppReport onlineAppReport = new OnlineAppReport();
    private long dedicatedTime;

    @Override
    public String getDescription() {
        return "Execute online for CB.";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        var outputCsvPath = Path.of(BenchConst.onlineCommandResultFile());
        var onlineReportPath = Path.of(BenchConst.onlineCommandOnlineReport());

        var isolationList = BenchConst.onlineCommandIsolationLevel();
        LOG.info("isolationList={}", isolationList);
        List<String> txList = BenchConst.onlineCommandTxOption();
        LOG.info("txList={}", txList);
        var batchDate = InitialData.DEFAULT_BATCH_DATE;
        LOG.info("batchDate={}", batchDate);
        var factoryList = StringUtil.toIntegerList(BenchConst.onlineCommandFactoryList());
        LOG.info("factoryList={}", StringUtil.toString(factoryList));
        var coverRateList = BenchConst.onlineCommandCoverRate();
        LOG.info("coverRateList={}", coverRateList);
        int times = BenchConst.onlineCommandExecuteTimes();
        LOG.info("times={}", times);
        int executeTime = BenchConst.onlineCommandExecuteTime();
        LOG.info("executeTime={}", executeTime);

        int exitCode = 0;
        var records = new ArrayList<OnlineResult>();
        for (var isolationLevel : isolationList) {
            for (var txOption : txList) {
                for (int coverRate : coverRateList) {
                    for (int i = 0; i < times; i++) {
                        var config = CostAccountingOnline.createDefaultConfig(batchDate, false);
                        config.setLabel(BenchConst.onlineCommandLabel());
                        config.setIsolationLevel(isolationLevel);
                        config.setCoverRate(coverRate);
                        setTxOption(config, txOption);
                        config.setExecuteTime(executeTime);
                        config.setEnableTsurugidbWatcher(BenchConst.tsurugiWatcherEnable());

                        exitCode |= execute1(config, i, records);

                        writeResult(outputCsvPath, records);
                        writeOnlineAppReport(config, records.get(records.size() - 1), onlineReportPath);
                    }
                }
            }
        }
        return exitCode;
    }

    private void setTxOption(OnlineConfig config, String txOption) {
        String[] split = txOption.split(":");
        if (split.length == 1) {
            String option = txOption.trim();
            config.setTxOption("online", option);
            config.setTxOption("periodic", option);
            return;
        }
        if (split.length == 2) {
            config.setTxOption("online", split[0].trim());
            config.setTxOption("periodic", split[1].trim());
            return;
        }
        throw new IllegalArgumentException("illegal txOption. online-command.tx.option=" + txOption);
    }

    private int execute1(OnlineConfig config, int attempt, List<OnlineResult> records) throws Exception {
        if (BenchConst.onlineCommandInitData()) {
            LOG.info("initdata start");
            InitialData.main();
            LOG.info("initdata end");
        }
        if (BenchConst.onlineCommandPreBatch()) {
            LOG.info("pre-batch start");
            var preConfig = new BatchConfig(DbManagerPurpose.PRE_BATCH, BenchConst.PARALLEL_FACTORY_SESSION, config.getBatchDate(), null, 100);
            preConfig.setIsolationLevel(IsolationLevel.READ_COMMITTED);
            preConfig.setDefaultTxOption(CostAccountingBatch.BATCH_LTX_OPTION);
            preConfig.setBatchFactoryOrder(BenchConst.getBatchFactoryOrder());
            CostAccountingBatch.initializeConfig(preConfig);

            var batch = new CostAccountingBatch();
            int exitCode = batch.main(preConfig);
            if (exitCode != 0) {
                throw new RuntimeException("pre-batch exitCode=" + exitCode);
            }
            LOG.info("pre-batch end");
        }
        CostBenchDbManager.initCounter();

        int exitCode;
        TsurugidbWatcher watcher;
        try (var watcherService = TsurugidbWatcherService.of(config.enableTsurugidbWatcher())) {
            watcher = watcherService.start();
            exitCode = execute1Main(config, attempt, records);
        }

        var record = records.get(records.size() - 1);
        if (watcher != null) {
            record.setMemInfo(watcher.getVsz(), watcher.getRss());
        }
        LOG.info("Finished. elapsed secs = {}.", record.elapsedMillis() / 1000.0);
        LOG.info("Counter infos: \n---\n{}---", CostBenchDbManager.createCounterReport());

        return exitCode;
    }

    private int execute1Main(OnlineConfig config, int attempt, List<OnlineResult> records) throws Exception {
        var online = new CostAccountingOnline();

        int exitCode = 0;
        Throwable occurred = null;
        try {
            var record = new OnlineResult(config, attempt);
            records.add(record);
            LOG.info("Executing with {}.", record.getParamString());

            record.start();
            online.start(config);

            LOG.info("sleep start");
            long startTime = System.currentTimeMillis();
            TimeUnit.SECONDS.sleep(config.getExecuteTime());
            this.dedicatedTime = System.currentTimeMillis() - startTime;
            LOG.info("sleep end");

            if (online.terminate(false) != 0) {
                if (exitCode == 0) {
                    exitCode = 2;
                }
            }
            online = null;
            record.finish();

            return exitCode;
        } catch (Throwable e) {
            occurred = e;
            LOG.error("execute1Main() error", e);
            throw e;
        } finally {
            if (online != null) {
                try {
                    if (online.terminate(true) != 0) {
                        if (exitCode == 0) {
                            return 2;
                        }
                    }
                } catch (Throwable e) {
                    if (occurred != null) {
                        occurred.addSuppressed(e);
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    private void writeResult(Path outputPath, List<OnlineResult> records) throws IOException {
        var dir = outputPath.getParent();
        if (dir != null) {
            Files.createDirectories(dir);
        }
        try (var pw = new PrintWriter(Files.newBufferedWriter(outputPath))) {
            pw.println(OnlineResult.header());
            for (var record : records) {
                pw.println(record);
            }
        }
    }

    private void writeOnlineAppReport(OnlineConfig config, OnlineResult record, Path onlineReportPath) {
        var title = OnlineAppReportHeader.ofOnline(record.dbmsType(), config.getLabel(), record.option("online"), record.option("periodic"), config.getCoverRate());
        onlineAppReport.writeOnlineAppReport(config, title, onlineReportPath, dedicatedTime, null);
    }
}
