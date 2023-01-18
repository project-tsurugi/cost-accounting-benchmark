package com.tsurugidb.benchmark.costaccounting.db;

import com.tsurugidb.benchmark.costaccounting.batch.task.BenchBatchTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.event.counter.TgTmLabelCounter;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class BenchDbCounter extends TgTmLabelCounter {

    /** カウンターの名称 */
    public enum CounterName {
        BEGIN_TX, TRY_COMMIT, SUCCESS, ABORTED
    }

    @Override
    protected String label(TgTxOption option) {
        String label = super.label(option);
        if (label.startsWith(BenchBatchTxOption.LABEL_PREFIX)) {
            return BenchBatchTxOption.LABEL_PREFIX;
        }
        return label;
    }

    public void increment(TgTmSetting setting, CounterName name) {
        var option = setting.getFirstTransactionOption();
        increment(option, name);
    }

    public void increment(TgTxOption option, CounterName name) {
        String label = label(option);
        var count = getOrCreate(label);
        switch (name) {
        case BEGIN_TX:
            count.incrementTransactionCount();
            break;
        case TRY_COMMIT:
            count.incrementBeforeCommitCount();
            break;
        case SUCCESS:
            count.incrementSuccessCommitCount();
            break;
        case ABORTED:
            count.incrementExceptionCount();
            break;
        default:
            throw new AssertionError(name);
        }
    }

    public int getCount(String label, CounterName name) {
        var countOpt = findCount(label);
        return countOpt.map(count -> {
            switch (name) {
            case BEGIN_TX:
                return count.transactionCount();
            case TRY_COMMIT:
                return count.beforeCommitCount();
            case SUCCESS:
                return count.successCommitCount();
            case ABORTED:
                return count.execptionCount();
            default:
                throw new AssertionError(name);
            }
        }).orElse(0);
    }
}
