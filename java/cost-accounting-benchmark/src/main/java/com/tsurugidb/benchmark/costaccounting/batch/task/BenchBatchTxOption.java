package com.tsurugidb.benchmark.costaccounting.batch.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TgTxOptionAlways;
import com.tsurugidb.iceaxe.transaction.TgTxState;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

public class BenchBatchTxOption extends TgTxOptionAlways {
    private static final Logger LOG = LoggerFactory.getLogger(BenchBatchTxOption.class);

    public BenchBatchTxOption() {
        super(TgTxOption.ofLTX(ResultTableDao.TABLE_NAME), 100);
    }

    @Override
    public TgTxState get(int attempt, TsurugiTransactionException e) {
        if (attempt > 0) {
            LOG.info("transaction retry{}", attempt);
        }
        return super.get(attempt, e);
    }
}
