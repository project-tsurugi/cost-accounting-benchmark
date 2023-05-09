package com.tsurugidb.benchmark.costaccounting.batch.command;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.batch.BatchConfig;
import com.tsurugidb.benchmark.costaccounting.batch.CostAccountingBatch;
import com.tsurugidb.benchmark.costaccounting.batch.StringUtil;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.init.DumpCsv;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.online.CostAccountingOnline;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.benchmark.costaccounting.online.command.OnlineAppReport;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.benchmark.costaccounting.watcher.TateyamaWatcher;
import com.tsurugidb.benchmark.costaccounting.watcher.TateyamaWatcherService;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class BatchCommand implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(BatchCommand.class);

    private Path baseResultFile;
    private final OnlineAppReport onlineAppReport = new OnlineAppReport();
    private long dedicatedTime;

    @Override
    public String getDescription() {
        return "Execute batch.";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        this.baseResultFile = null;
        var outputPath = Path.of(BenchConst.batchCommandResultFile());

        var executeList = Arrays.stream(BenchConst.batchCommandExecuteType().split(",")).map(String::trim).collect(Collectors.toList());
        LOG.info("executeList={}", executeList);
        var isolationList = BenchConst.batchCommandIsolationLevel();
        LOG.info("isolationList={}", isolationList);
        List<String> txList = BenchConst.batchCommandTxOption();
        LOG.info("txList={}", txList);
        var batchDate = InitialData.DEFAULT_BATCH_DATE;
        LOG.info("batchDate={}", batchDate);
        var factoryList = StringUtil.toIntegerList(BenchConst.batchCommandFactoryList());
        LOG.info("factoryList={}", StringUtil.toString(factoryList));
        int times = BenchConst.batchCommandExecuteTimes();
        LOG.info("times={}", times);

        int exitCode = 0;
        var records = new ArrayList<BatchRecord>();
        for (var executeType : executeList) {
            for (var isolationLevel : isolationList) {
                for (var txOption : txList) {
                    for (int i = 0; i < times; i++) {
                        var config = new BatchConfig(executeType, batchDate, factoryList, 100);
                        config.setIsolationLevel(isolationLevel);
                        config.setDefaultTxOption(getOption(txOption));

                        OnlineConfig onlineConfig = null;
                        if (BenchConst.batchCommandOnline()) {
                            onlineConfig = CostAccountingOnline.createDefaultConfig(batchDate);
                        }
                        exitCode |= execute1(config, onlineConfig, i, records);

                        writeResult(outputPath, records);
                        if (BenchConst.batchCommandOnline()) {
                            writeOnlineAppReport(onlineConfig, records.get(records.size() - 1), outputPath);
                        }
                    }
                }
            }
        }
        return exitCode;
    }

    private int execute1(BatchConfig config, OnlineConfig onlineConfig, int attempt, List<BatchRecord> records) throws Exception {
        if (BenchConst.batchCommandInitData()) {
            LOG.info("initdata start");
            InitialData.main();
            LOG.info("initdata end");
        }
        if (BenchConst.batchCommandPreBatch()) {
            LOG.info("pre-batch start");
            var preConfig = new BatchConfig(BenchConst.PARALLEL_FACTORY_SESSION, config.getBatchDate(), config.getFactoryList(), 100);
            preConfig.setIsolationLevel(IsolationLevel.READ_COMMITTED);
            preConfig.setDefaultTxOption(getOption("LTX"));

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
            exitCode = execute1Main(config, onlineConfig, attempt, records);
        }

        var record = records.get(records.size() - 1);
        if (watcher != null) {
            record.setMemInfo(watcher.getVsz(), watcher.getRss());
        }
        LOG.info("Finished. elapsed secs = {}.", record.elapsedMillis() / 1000.0);
        LOG.info("Counter infos: \n---\n{}---", CostBenchDbManager.createCounterReport());

        diff(record);

        return exitCode;
    }

    private int execute1Main(BatchConfig config, OnlineConfig onlineConfig, int attempt, List<BatchRecord> records) throws Exception {
        CostAccountingOnline online = null;
        if (onlineConfig != null) {
            online = new CostAccountingOnline();
        }

        int exitCode = 0;
        try {
            var record = new BatchRecord(config, attempt);
            records.add(record);
            LOG.info("Executing with {}.", record.getParamString());

            var batch = new CostAccountingBatch();
            if (online != null) {
                online.start(onlineConfig);
            }

            record.start();
            exitCode = batch.main(config);
            record.finish(batch.getItemCount(), batch.getTryCount(), batch.getAbortCount());
            this.dedicatedTime = record.elapsedMillis();

            if (online != null) {
                if (online.terminate() != 0) {
                    if (exitCode == 0) {
                        exitCode = 2;
                    }
                }
                online = null;
            }

            return exitCode;
        } finally {
            if (online != null) {
                online.terminate();
            }
        }
    }

    private TgTxOption getOption(String s) {
        switch (s) {
        case "OCC":
            return TgTxOption.ofOCC();
        default:
            return TgTxOption.ofLTX(ResultTableDao.TABLE_NAME);
        }
    }

    private void diff(BatchRecord record) {
        Path outputDir;
        try {
            String s = BenchConst.batchCommandDiffDir();
            LOG.trace("batch-command.diff.dir={}", s);
            if (s == null) {
                record.setDiff("-");
                return;
            }
            outputDir = Path.of(s);
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }

        LOG.info("diff start");

        LOG.debug("dump csv start");
        Path outputFile = outputDir.resolve(ResultTableDao.TABLE_NAME + "." + record.dbmsType() + "." + record.executeType() + "." + record.option() + "." + record.attempt() + ".csv");
        try {
            new DumpCsv().dump(outputFile, ResultTableDao.TABLE_NAME);
        } catch (IOException e) {
            LOG.warn("dump csv error. file={}", outputFile, e);
            record.setDiff("dump error");
            return;
        }
        LOG.debug("dump csv end");

        if (this.baseResultFile == null) {
            this.baseResultFile = outputFile;
            record.setDiff(0);
        } else {
            LOG.debug("diff check start");
            try (var baseStream = Files.lines(baseResultFile, StandardCharsets.UTF_8); //
                    var testStream = Files.lines(outputFile, StandardCharsets.UTF_8)) {
                var baseSet = baseStream.collect(Collectors.toSet());
                var testSet = new HashSet<String>();
                testStream.forEach(s -> {
                    if (!baseSet.remove(s)) {
                        testSet.add(s);
                    }
                });
                record.setDiff(baseSet.size() + testSet.size());
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            LOG.debug("diff check end");
        }

        LOG.info("diff end");
    }

    private void writeResult(Path outputPath, List<BatchRecord> records) throws IOException {
        var dir = outputPath.getParent();
        if (dir != null) {
            Files.createDirectories(dir);
        }
        try (var pw = new PrintWriter(Files.newBufferedWriter(outputPath))) {
            pw.println(BatchRecord.header());
            for (var record : records) {
                pw.println(record);
            }
        }
    }

    private void writeOnlineAppReport(OnlineConfig onlineConfig, BatchRecord record, Path outputPath) {
        String title = record.dbmsType().name() + " " + record.factory() + " " + record.scope() + " " + record.option();
        onlineAppReport.writeOnlineAppReport(onlineConfig, title, outputPath, dedicatedTime);
    }
}
