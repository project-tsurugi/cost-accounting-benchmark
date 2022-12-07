package com.tsurugidb.benchmark.costaccounting.batch.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.batch.BatchConfig;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTxOptionAlways;

public class BenchBatchTxOption extends TgTxOptionAlways {
    private static final Logger LOG = LoggerFactory.getLogger(BenchBatchTxOption.class);

    public static BenchBatchTxOption of(BatchConfig config) {
        return new BenchBatchTxOption(getTxOption(config, 0));
    }

    public static BenchBatchTxOption of(BatchConfig config, int factoryId) {
        return new BenchBatchTxOption(getTxOption(config, factoryId));
    }

    private static TgTxOption getTxOption(BatchConfig config, int factoryId) {
        var option = config.getTxOption(factoryId).clone();
        option.label("batch" + factoryId);
        return option;
    }

    public BenchBatchTxOption(TgTxOption option) {
        super(option, 100);
        setStateListener((attempt, e, state) -> {
            if (attempt > 0) {
                if (state.isExecute()) {
                    LOG.info("transaction retry. attempt={} {}", attempt, e.getMessage());
                } else {
                    LOG.info("transaction error. attempt={} {}", attempt, e.getMessage());
                }
            }
        });
    }

    @Override
    public String toString() {
        return transactionOption.toString();
    }
}
