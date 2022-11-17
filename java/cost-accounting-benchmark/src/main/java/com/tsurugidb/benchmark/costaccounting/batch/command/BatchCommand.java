package com.tsurugidb.benchmark.costaccounting.batch.command;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.batch.BatchConfig;
import com.tsurugidb.benchmark.costaccounting.batch.CostAccountingBatch;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

public class BatchCommand implements ExecutableCommand {

    @Override
    public String getDescription() {
        return "Execute batch.";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        var executeList = List.of("sequential-single-tx", "sequential-factory-tx", "parallel-single-tx", "parallel-factory-tx");
        var txList = List.of(TgTxOption.ofOCC(), TgTxOption.ofLTX(ResultTableDao.TABLE_NAME));
        var batchDate = InitialData.DEFAULT_BATCH_DATE;
        var factoryList = IntStream.rangeClosed(1, 8).boxed().collect(Collectors.toList());

        int exitCode = 0;
        for (var executeType : executeList) {
            for (var txOption : txList) {
                var config = new BatchConfig(executeType, batchDate, factoryList, 100);
                config.setDefaultTxOption(txOption);

                exitCode |= new CostAccountingBatch().main(config);
                // TODO report
            }
        }
        return exitCode;
    }
}
