/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.benchmark.costaccounting.batch.task;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.batch.BatchConfig;
import com.tsurugidb.iceaxe.exception.TsurugiExceptionUtil;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOptionAlways;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryInstruction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class BenchBatchTxOption extends TgTmTxOptionAlways {
    private static final Logger LOG = LoggerFactory.getLogger(BenchBatchTxOption.class);

    public static final String LABEL_PREFIX = "batch";

    public static BenchBatchTxOption of(BatchConfig config) {
        return new BenchBatchTxOption(getTxOption(config, 0));
    }

    public static BenchBatchTxOption of(BatchConfig config, int factoryId) {
        return new BenchBatchTxOption(getTxOption(config, factoryId));
    }

    private static TgTxOption getTxOption(BatchConfig config, int factoryId) {
        var option = config.getTxOption(factoryId).clone();
        option.label(String.format("%s%03d", LABEL_PREFIX, factoryId));
        return option;
    }

    public BenchBatchTxOption(TgTxOption option) {
        super(option, 1000);
        setTmOptionListener((attempt, e, tmOption) -> {
            if (attempt > 0) {
                if (tmOption.isExecute()) {
                    LOG.info("transaction retry. attempt={} {}", attempt, e.getMessage());
                } else {
                    LOG.info("transaction error. attempt={} {}", attempt, e.getMessage());
                }
            }
        });
    }

    @Override
    protected TgTmRetryInstruction isRetryable(TsurugiTransaction transaction, TsurugiTransactionException e) throws IOException, InterruptedException {
        var exceptionUtil = TsurugiExceptionUtil.getInstance();
        if (exceptionUtil.isInactiveTransaction(e)) {
            var code = e.getDiagnosticCode();
            return TgTmRetryInstruction.ofRetryable(code);
        }

        return super.isRetryable(transaction, e);
    }

    @Override
    public String toString() {
        return txOption.toString();
    }
}
