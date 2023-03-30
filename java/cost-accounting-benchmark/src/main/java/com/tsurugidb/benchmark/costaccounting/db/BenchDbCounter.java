package com.tsurugidb.benchmark.costaccounting.db;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.tsurugidb.benchmark.costaccounting.batch.task.BenchBatchTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.event.counter.TgTmCount;
import com.tsurugidb.iceaxe.transaction.manager.event.counter.TgTmLabelCounter;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class BenchDbCounter extends TgTmLabelCounter {

    /** カウンターの名称 */
    public enum CounterName {
        // for batch
        BEGIN_TX, TRY_COMMIT, SUCCESS, ABORTED,
        // for online
        OCC_TRY, OCC_ABORT, OCC_SUCCESS, OCC_ABANDONED_RETRY, //
        LTX_TRY, LTX_ABORT, LTX_SUCCESS, LTX_ABANDONED_RETRY, //
        FAIL
    }

    private static class OnlineCounter {
        private AtomicInteger occTry = new AtomicInteger(0);
        private AtomicInteger occAbort = new AtomicInteger(0);
        private AtomicInteger occSuccess = new AtomicInteger(0);
        private AtomicInteger occAbandonedRetry = new AtomicInteger(0);
        private AtomicInteger ltxTry = new AtomicInteger(0);
        private AtomicInteger ltxAbort = new AtomicInteger(0);
        private AtomicInteger ltxSuccess = new AtomicInteger(0);
        private AtomicInteger ltxAbandonedRetry = new AtomicInteger(0);
    }

    private final Map<String, OnlineCounter> onlineCounterMap = new ConcurrentHashMap<>();

    // for Iceaxe

    @Override
    protected String label(TgTxOption txOption) {
        String label = super.label(txOption);
        if (label.startsWith(BenchBatchTxOption.LABEL_PREFIX)) {
            return BenchBatchTxOption.LABEL_PREFIX;
        }
        return label;
    }

    private OnlineCounter getOnlineCounter(TgTxOption txOption) {
        String label = label(txOption);
        return onlineCounterMap.computeIfAbsent(label, k -> new OnlineCounter());
    }

    @Override
    public void transactionStart(TsurugiTransactionManager tm, int iceaxeTmExecuteId, int attempt, TgTxOption txOption) {
        super.transactionStart(tm, iceaxeTmExecuteId, attempt, txOption);

        var counter = getOnlineCounter(txOption);
        if (txOption.isOCC()) {
            counter.occTry.incrementAndGet();
        } else {
            counter.ltxTry.incrementAndGet();
        }
    }

    @Override
    public void transactionException(TsurugiTransaction transaction, Throwable e) {
        super.transactionException(transaction, e);

        var txOption = transaction.getTransactionOption();
        var counter = getOnlineCounter(txOption);
        if (txOption.isOCC()) {
            counter.occAbort.incrementAndGet();
        } else {
            counter.ltxAbort.incrementAndGet();
        }
    }

    @Override
    public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTxOption nextTxOption) {
        super.transactionRetry(transaction, cause, nextTxOption);

        var txOption = transaction.getTransactionOption();
        if (txOption.isOCC() && !nextTxOption.isOCC()) {
            var counter = getOnlineCounter(txOption);
            counter.occAbandonedRetry.incrementAndGet();
        }
    }

    @Override
    public void transactionRetryOver(TsurugiTransaction transaction, Exception cause) {
        super.transactionRetryOver(transaction, cause);

        var txOption = transaction.getTransactionOption();
        var counter = getOnlineCounter(txOption);
        if (txOption.isOCC()) {
            counter.occAbandonedRetry.incrementAndGet();
        } else {
            counter.ltxAbandonedRetry.incrementAndGet();
        }
    }

    @Override
    public void executeEndSuccess(TsurugiTransaction transaction, boolean committed, Object returnValue) {
        super.executeEndSuccess(transaction, committed, returnValue);

        var txOption = transaction.getTransactionOption();
        var counter = getOnlineCounter(txOption);
        if (txOption.isOCC()) {
            counter.occSuccess.incrementAndGet();
        } else {
            counter.ltxSuccess.incrementAndGet();
        }
    }

    // for JDBC

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

    // result

    public int getCount(String label, CounterName name) {
        switch (name) {
        case BEGIN_TX:
            return getCount(label, TgTmCount::transactionCount);
        case TRY_COMMIT:
            return getCount(label, TgTmCount::beforeCommitCount);
        case SUCCESS:
            return getCount(label, TgTmCount::successCommitCount);
        case ABORTED:
            return getCount(label, TgTmCount::execptionCount);
        case OCC_TRY:
            return getOnlineCount(label, counter -> counter.occTry.get());
        case OCC_SUCCESS:
            return getOnlineCount(label, counter -> counter.occSuccess.get());
        case OCC_ABORT:
            return getOnlineCount(label, counter -> counter.occAbort.get());
        case OCC_ABANDONED_RETRY:
            return getOnlineCount(label, counter -> counter.occAbandonedRetry.get());
        case LTX_TRY:
            return getOnlineCount(label, counter -> counter.ltxTry.get());
        case LTX_SUCCESS:
            return getOnlineCount(label, counter -> counter.ltxSuccess.get());
        case LTX_ABORT:
            return getOnlineCount(label, counter -> counter.ltxAbort.get());
        case LTX_ABANDONED_RETRY:
            return getOnlineCount(label, counter -> counter.ltxAbandonedRetry.get());
        case FAIL:
            return getCount(label, TgTmCount::failCount);
        default:
            throw new AssertionError(name);
        }
    }

    private int getCount(String label, Function<TgTmCount, Integer> task) {
        var countOpt = findCount(label);
        return countOpt.map(task).orElse(0);
    }

    private int getOnlineCount(String label, Function<OnlineCounter, Integer> task) {
        var counter = onlineCounterMap.get(label);
        if (counter == null) {
            return 0;
        }
        return task.apply(counter);
    }

    @Override
    public void reset() {
        super.reset();

        onlineCounterMap.clear();
    }
}
