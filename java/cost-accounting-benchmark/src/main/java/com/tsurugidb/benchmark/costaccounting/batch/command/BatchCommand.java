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
        return "Execute batch. arg1=<result file path>";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        if (args.length <= 1) {
            System.err.println("ERROR: too few arguments.");
            System.err.println(getDescription());
            return 1;
        }
        var outputPath = Path.of(args[1]);

        var executeList = Arrays.stream(BenchConst.batchCommandExecuteType().split(",")).map(String::trim).collect(Collectors.toList());
        LOG.info("executeList={}", executeList);
        var isolationList = BenchConst.batchCommandIsolationLevel();
        LOG.info("isolationList={}", isolationList);
        List<TgTxOption> txList;
        switch (BenchConst.dbmsType()) {
        case TSURUGI:
            txList = List.of(TgTxOption.ofOCC(), TgTxOption.ofLTX(ResultTableDao.TABLE_NAME));
            break;
        default:
            txList = List.of(TgTxOption.ofOCC());
            break;
        }
        LOG.info("txList={}", txList);
        var batchDate = InitialData.DEFAULT_BATCH_DATE;
        LOG.info("batchDate={}", batchDate);
        var factoryList = StringUtil.toIntegerList(BenchConst.batchCommandFactoryList());
        LOG.info("factoryList={}", StringUtil.toString(factoryList));

        int exitCode = 0;
        var records = new ArrayList<BatchRecord>();
        for (var executeType : executeList) {
            for (var isolationLevel : isolationList) {
                for (var txOption : txList) {
                    var config = new BatchConfig(executeType, batchDate, factoryList, 100);
                    config.setIsolationLevel(isolationLevel);
                    config.setDefaultTxOption(txOption);

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
        return exitCode;
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