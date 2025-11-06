/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.costaccounting.db;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.tsurugidb.benchmark.costaccounting.batch.task.BenchBatchTxOption;
import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.exception.TsurugiExceptionUtil;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.event.counter.TgTmCount;
import com.tsurugidb.iceaxe.transaction.manager.event.counter.TgTmLabelCounter;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class BenchDbCounter extends TgTmLabelCounter {

    /** カウンターの名称 */
    public enum CounterName {
        // for batch
        BEGIN_TX, TRY_COMMIT, SUCCESS, ABORTED,
        // for online
        OCC_TRY, OCC_ABORT, OCC_SUCCESS, OCC_ABANDONED_RETRY, //
        LTX_TRY, LTX_ABORT, LTX_SUCCESS, LTX_ABANDONED_RETRY, //
        CONFLIT_ON_WP, OCC_WP_VERIFY, //
        FAIL, //
        TASK_START, TASK_NOTHING, TASK_SUCCESS, TASK_FAIL
    }

    private static class OnlineCounter {
        private String txOptionDescription;

        private AtomicInteger occTry = new AtomicInteger(0);
        private AtomicInteger occAbort = new AtomicInteger(0);
        private AtomicInteger conflictOnWp = new AtomicInteger(0);
        private AtomicInteger occWpVerify = new AtomicInteger(0);
        private AtomicInteger occSuccess = new AtomicInteger(0);
        private AtomicInteger occAbandonedRetry = new AtomicInteger(0);
        private AtomicInteger ltxTry = new AtomicInteger(0);
        private AtomicInteger ltxAbort = new AtomicInteger(0);
        private AtomicInteger ltxSuccess = new AtomicInteger(0);
        private AtomicInteger ltxAbandonedRetry = new AtomicInteger(0);
        private AtomicInteger taskStart = new AtomicInteger(0);
        private AtomicInteger taskNothing = new AtomicInteger(0);
        private AtomicInteger taskSuccess = new AtomicInteger(0);
        private AtomicInteger taskFail = new AtomicInteger(0);

        private final OnlineTime taskNothingTime = new OnlineTime();
        private final OnlineTime taskSuccessTime = new OnlineTime();
        private final OnlineTime taskFailTime = new OnlineTime();
    }

    public static class OnlineTime {
        private int count = 0;
        private long timeSum = 0;
        private long timeMin = Long.MAX_VALUE;
        private long timeMax = Long.MIN_VALUE;

        public synchronized void addTime(long nanoTime) {
            this.count++;
            this.timeSum += nanoTime;
            this.timeMin = Math.min(timeMin, nanoTime);
            this.timeMax = Math.max(timeMax, nanoTime);
        }

        public synchronized int getCount() {
            return this.count;
        }

        public synchronized double getAvgTimeMillis() {
            if (this.count == 0) {
                return Double.NaN;
            }
            return (double) timeSum / count / 1000_000;
        }

        public synchronized double getMinTimeMillis() {
            if (this.timeMin == Long.MAX_VALUE) {
                return Double.NaN;
            }
            return timeMin / 1000_000d;
        }

        public synchronized double getMaxTimeMillis() {
            if (this.timeMax == Long.MIN_VALUE) {
                return Double.NaN;
            }
            return timeMax / 1000_000d;
        }
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
        return getOnlineCounter(label);
    }

    private OnlineCounter getOnlineCounter(String label) {
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

        if (e instanceof TsurugiDiagnosticCodeProvider) {
            var t = (TsurugiDiagnosticCodeProvider) e;
            var exceptionUtil = TsurugiExceptionUtil.getInstance();
            if (exceptionUtil.isConflictOnWritePreserve(t)) {
                counter.conflictOnWp.incrementAndGet();
            }
            if (exceptionUtil.isSerializationFailure(t)) {
                String message = e.getMessage();
                if (message.contains("shirakami response Status=ERR_CC")) {
                    if (message.contains("reason_code:CC_OCC_WP_VERIFY")) {
                        counter.occWpVerify.incrementAndGet();
                    }
                }
            }
        }
    }

    @Override
    public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
        super.transactionRetry(transaction, cause, nextTmOption);

        var txOption = transaction.getTransactionOption();
        if (txOption.isOCC() && !nextTmOption.getTransactionOption().isOCC()) {
            var counter = getOnlineCounter(txOption);
            counter.occAbandonedRetry.incrementAndGet();
        }
    }

    @Override
    public void transactionRetryOver(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
        super.transactionRetryOver(transaction, cause, nextTmOption);

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
        var info = setting.getTransactionOptionSupplier().createExecuteInfo(0);
        try {
            var option = setting.getFirstTransactionOption(info);
            increment(option, name);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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

    // for task
    public boolean setTxOptionDescription(String label, String description) {
        var count = getOnlineCounter(label);
        synchronized (count) {
            boolean result = (count.txOptionDescription == null);
            count.txOptionDescription = description;
            return result;
        }
    }

    public void increment(String label, CounterName name) {
        var count = getOnlineCounter(label);
        switch (name) {
        case TASK_START:
            count.taskStart.incrementAndGet();
            break;
        case TASK_NOTHING:
            count.taskNothing.incrementAndGet();
            break;
        case TASK_SUCCESS:
            count.taskSuccess.incrementAndGet();
            break;
        case TASK_FAIL:
            count.taskFail.incrementAndGet();
            break;
        default:
            throw new AssertionError(name);
        }
    }

    public void addTime(String label, CounterName name, long nanoTime) {
        var count = getOnlineCounter(label);
        switch (name) {
        case TASK_NOTHING:
            count.taskNothingTime.addTime(nanoTime);
            break;
        case TASK_SUCCESS:
            count.taskSuccessTime.addTime(nanoTime);
            break;
        case TASK_FAIL:
            count.taskFailTime.addTime(nanoTime);
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
            return getCount(label, TgTmCount::exceptionCount);
        case OCC_TRY:
            return getOnlineCount(label, counter -> counter.occTry.get());
        case OCC_SUCCESS:
            return getOnlineCount(label, counter -> counter.occSuccess.get());
        case OCC_ABORT:
            return getOnlineCount(label, counter -> counter.occAbort.get());
        case CONFLIT_ON_WP:
            return getOnlineCount(label, counter -> counter.conflictOnWp.get());
        case OCC_WP_VERIFY:
            return getOnlineCount(label, counter -> counter.occWpVerify.get());
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
        case TASK_START:
            return getOnlineCount(label, counter -> counter.taskStart.get());
        case TASK_NOTHING:
            return getOnlineCount(label, counter -> counter.taskNothing.get());
        case TASK_SUCCESS:
            return getOnlineCount(label, counter -> counter.taskSuccess.get());
        case TASK_FAIL:
            return getOnlineCount(label, counter -> counter.taskFail.get());
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

    public String getTxOptionDescription(String label) {
        var count = getOnlineCounter(label);
        return count.txOptionDescription;
    }

    public OnlineTime getTime(String label, CounterName name) {
        var count = getOnlineCounter(label);
        switch (name) {
        case TASK_NOTHING:
            return count.taskNothingTime;
        case TASK_SUCCESS:
            return count.taskSuccessTime;
        case TASK_FAIL:
            return count.taskFailTime;
        default:
            throw new AssertionError(name);
        }
    }

    @Override
    public void reset() {
        super.reset();

        onlineCounterMap.clear();
    }
}
