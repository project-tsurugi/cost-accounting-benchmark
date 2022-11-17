package com.tsurugidb.benchmark.costaccounting.batch.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.batch.BatchConfig;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTxOptionAlways;
import com.tsurugidb.iceaxe.transaction.manager.TgTxState;

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
    }

    @Override
    public TgTxState get(int attempt, TsurugiTransactionException e) {
        if (attempt > 0) {
            LOG.info("transaction error. attempt={} {}", attempt - 1, e.getMessage());
        }
        return super.get(attempt, e);
    }

    @Override
    public String toString() {
        return transactionOption.toString();
    }
}
