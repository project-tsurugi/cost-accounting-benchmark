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
package com.tsurugidb.benchmark.costaccounting.batch.command;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class BatchRecordReadThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(BatchRecordReadThread.class);

    private final BatchRecord record;

    public BatchRecordReadThread(BatchRecord record) {
        this.record = record;
    }

    @Override
    public void run() {
        try {
            Path compareBaseFile = BenchConst.batchCommandBatchCompareBase();
            LOG.info("compareBaseFile={}", compareBaseFile);
            if (compareBaseFile == null) {
                return;
            }

            List<String> textList = Files.readAllLines(compareBaseFile, StandardCharsets.UTF_8);
            var compareBaseRecord = getCompareBase(textList);
            LOG.info("batch compare base={}", compareBaseRecord);
            record.setCompareBaseRecord(compareBaseRecord);
        } catch (Exception e) {
            LOG.warn("compareBase read error", e);
        }
    }

    private BatchRecordPart getCompareBase(List<String> textList) {
        List<BatchRecordPart> list = textList.stream().map(line -> BatchRecord.parse(line)).filter(Objects::nonNull).collect(Collectors.toList());
        if (list.size() == 1) {
            return list.get(0);
        }

        { // option
            String option = record.option();
            list = filter(list, it -> Objects.equals(it.getOption(), option));
            if (list.size() == 1) {
                return list.get(0);
            }
        }

        LOG.warn("can't decide on one. {}", list);
        return list.get(0);
    }

    private List<BatchRecordPart> filter(List<BatchRecordPart> list, Predicate<BatchRecordPart> predicate) {
        List<BatchRecordPart> result = list.stream().filter(predicate).collect(Collectors.toList());
        if (!result.isEmpty()) {
            return result;
        }
        return list;
    }
}
