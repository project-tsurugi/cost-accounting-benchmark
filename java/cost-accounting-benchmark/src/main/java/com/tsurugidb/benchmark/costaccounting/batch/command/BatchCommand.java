package com.tsurugidb.benchmark.costaccounting.batch.command;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.batch.BatchConfig;
import com.tsurugidb.benchmark.costaccounting.batch.CostAccountingBatch;
import com.tsurugidb.benchmark.costaccounting.batch.StringUtil;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

public class BatchCommand implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(BatchCommand.class);

    @Override
    public String getDescription() {
        return "Execute batch.";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
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

                        var record = new BatchRecord(config);
                        records.add(record);
                        LOG.info("Executing with {}.", record.getParamString());

                        var batch = new CostAccountingBatch();
                        record.start();
                        exitCode |= batch.main(config);
                        record.finish(batch.getTryCount(), batch.getAbortCount());

                        LOG.info("Finished. elapsed secs = {}.", record.elapsedMillis() / 1000.0);
                        writeResult(outputPath, records);
                    }
                }
            }
        }
        return exitCode;
    }

    private TgTxOption getOption(String s) {
        switch (s) {
        case "OCC":
            return TgTxOption.ofOCC();
        default:
            return TgTxOption.ofLTX(ResultTableDao.TABLE_NAME);
        }
    }

    private void writeResult(Path outputPath, List<BatchRecord> records) throws IOException {
        try (var pw = new PrintWriter(Files.newBufferedWriter(outputPath))) {
            pw.println(BatchRecord.header());
            for (var record : records) {
                pw.println(record);
            }
        }
    }
}
