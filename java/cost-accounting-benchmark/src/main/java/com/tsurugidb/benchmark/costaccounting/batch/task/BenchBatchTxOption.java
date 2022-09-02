package com.tsurugidb.benchmark.costaccounting.batch.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.batch.StringUtil;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTxOptionAlways;
import com.tsurugidb.iceaxe.transaction.manager.TgTxState;

public class BenchBatchTxOption extends TgTxOptionAlways {
    private static final Logger LOG = LoggerFactory.getLogger(BenchBatchTxOption.class);

    public static BenchBatchTxOption of() {
        return new BenchBatchTxOption(getTxOption(0));
    }

    public static BenchBatchTxOption of(int factoryId) {
        return new BenchBatchTxOption(getTxOption(factoryId));
    }

    private static TgTxOption getTxOption(int factoryId) {
        var option = getTxOptionBase(factoryId);
        option.label("batch" + factoryId);
        return option;
    }

    private static TgTxOption getTxOptionBase(int factoryId) {
        String s = BenchConst.batchTsurugiTxOption().toUpperCase();

        int n = s.indexOf('[');
        if (n >= 0) {
            int m = s.indexOf(']', n);
            var list = StringUtil.toIntegerList(s.substring(n + 1, m));
            if (list.contains(factoryId) || factoryId == 0) {
                return s.startsWith("LTX") ? createTxOption("LTX") : createTxOption("OCC");
            } else {
                return s.startsWith("LTX") ? createTxOption("OCC") : createTxOption("LTX");
            }
        }

        return createTxOption(s);
    }

    private static TgTxOption createTxOption(String s) {
        switch (s) {
        case "LTX":
            return TgTxOption.ofLTX(ResultTableDao.TABLE_NAME);
        case "OCC":
            return TgTxOption.ofOCC();
        default:
            throw new UnsupportedOperationException(s);
        }
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
