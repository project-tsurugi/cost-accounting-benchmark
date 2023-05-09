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
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.online.CostAccountingOnline;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.benchmark.costaccounting.watcher.TateyamaWatcher;
import com.tsurugidb.benchmark.costaccounting.watcher.TateyamaWatcherService;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class OnlineCommand implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineCommand.class);

    private final OnlineAppReport onlineAppReport = new OnlineAppReport();
    private long dedicatedTime;

    @Override
    public String getDescription() {
        return "Execute online.";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        var outputPath = Path.of(BenchConst.onlineCommandResultFile());

        var isolationList = BenchConst.onlineCommandIsolationLevel();
        LOG.info("isolationList={}", isolationList);
        List<String> txList = BenchConst.onlineCommandTxOption();
        LOG.info("txList={}", txList);
        var batchDate = InitialData.DEFAULT_BATCH_DATE;
        LOG.info("batchDate={}", batchDate);
        var factoryList = StringUtil.toIntegerList(BenchConst.onlineCommandFactoryList());
        LOG.info("factoryList={}", StringUtil.toString(factoryList));
        int times = BenchConst.onlineCommandExecuteTimes();
        LOG.info("times={}", times);
        int executeTime = BenchConst.onlineCommandExecuteTime();
        LOG.info("executeTime={}", executeTime);

        int exitCode = 0;
        var records = new ArrayList<OnlineResult>();
        for (var isolationLevel : isolationList) {
            for (var txOption : txList) {
                for (int i = 0; i < times; i++) {
                    var config = CostAccountingOnline.createDefaultConfig(batchDate, false);
                    config.setLabel(BenchConst.onlineCommandLabel());
                    config.setIsolationLevel(isolationLevel);
                    setTxOption(config, txOption);
                    config.setExecuteTime(executeTime);

                    exitCode |= execute1(config, i, records);

                    writeResult(outputPath, records);
                    writeOnlineAppReport(config, records.get(records.size() - 1), outputPath);
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
            var preConfig = new BatchConfig(BenchConst.PARALLEL_FACTORY_SESSION, config.getBatchDate(), null, 100);
            preConfig.setIsolationLevel(IsolationLevel.READ_COMMITTED);
            preConfig.setDefaultTxOption(TgTxOption.ofLTX(ResultTableDao.TABLE_NAME));

            var batch = new CostAccountingBatch();
            int exitCode = batch.main(preConfig);
            if (exitCode != 0) {
                throw new RuntimeException("pre-batch exitCode=" + exitCode);
            }
            LOG.info("pre-batch end");
        }
        CostBenchDbManager.initCounter();

        int exitCode;
        TateyamaWatcher watcher;
        try (var watcherService = TateyamaWatcherService.of()) {
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

            if (online.terminate() != 0) {
                if (exitCode == 0) {
                    exitCode = 2;
                }
                online = null;
            }
            record.finish();

            return exitCode;
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            if (online != null) {
                if (online.terminate() != 0) {
                    if (occurred == null) {
                        if (exitCode == 0) {
                            return 2;
                        }
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

    private void writeOnlineAppReport(OnlineConfig config, OnlineResult record, Path outputPath) {
        String title = record.dbmsType().name() + " " + config.getLabel() + " " + record.option("online") + ":" + record.option("periodic");
        onlineAppReport.writeOnlineAppReport(config, title, outputPath, dedicatedTime);
    }
}
