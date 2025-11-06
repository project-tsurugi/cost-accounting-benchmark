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
package com.tsurugidb.benchmark.costaccounting.online.command;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.benchmark.costaccounting.online.periodic.BenchPeriodicTask;
import com.tsurugidb.benchmark.costaccounting.online.periodic.BenchPeriodicUpdateStockTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineTask;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.PathUtil;

public class OnlineAppReport {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineAppReport.class);

    private static final String HEADER1 = "# Online Application Report";

    private final StringBuilder onlineAppReport = new StringBuilder(2048).append(HEADER1 + "\n");
    private final StringBuilder csvReport = new StringBuilder(2048).append(OnlineTimeRecord.CSV_HEADER + "\n");

    public void writeOnlineAppReport(OnlineConfig config, OnlineAppReportHeader title, Path onlineReportPath, long dedicatedTime, Path compareBaseFile) {
        LOG.debug("Creating an online application report for {}", title);

        String newReport = createOnlineAppReport(config, title, dedicatedTime, compareBaseFile);
        LOG.debug("Online application report: {}", newReport);
        this.onlineAppReport.append(newReport);

        LOG.debug("Writing online application reports to {}", onlineReportPath.toAbsolutePath());
        try {
            Files.writeString(onlineReportPath, onlineAppReport);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }

        Path csvReportPath = PathUtil.convertExt(onlineReportPath, "csv");
        LOG.debug("Writing online application csv reports to {}", csvReportPath.toAbsolutePath());
        try {
            Files.writeString(csvReportPath, csvReport.toString());
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    private String createOnlineAppReport(OnlineConfig config, OnlineAppReportHeader title, long dedicatedTime, Path compareBaseFile) {
        var sb = new StringBuilder(2048);
        sb.append("\n");

        // タイトル
        sb.append("## ");
        sb.append(title);
        sb.append("\n\n");

        // counter
        appendHeader(sb, OnlineCounterRecord.HEADER_LIST);
        for (String taskName : BenchOnlineTask.TASK_NAME_LIST) {
            createOnlineCounterReport(config, sb, taskName);
        }
        for (String taskName : BenchPeriodicTask.TASK_NAME_LIST) {
            createOnlineCounterReport(config, sb, taskName);
        }
        sb.append("\n");
        sb.append(OnlineCounterRecord.FOOTER);
        sb.append("\n");

        sb.append("\n");

        // time
        OnlineAppReportHeader compareBaseKey = null;
        Map<String, OnlineTimeRecord> compareBaseMap = Map.of();
        {
            LOG.info("compareBaseFile={}", compareBaseFile);
            if (compareBaseFile != null) {
                var allMap = readCompareBaseFile(compareBaseFile);
                var key = getCompareBaseKey(config, title, allMap);
                if (key != null) {
                    LOG.info("compare base=[{}]", key);
                    compareBaseKey = key;
                    compareBaseMap = allMap.get(compareBaseKey);
                }
            }
        }
        appendHeader(sb, OnlineTimeRecord.HEADER_LIST);
        for (String taskName : BenchOnlineTask.TASK_NAME_LIST) {
            var compareBaseRecord = compareBaseMap.get(taskName);
            cerateOnlineTimeReport(config, sb, taskName, dedicatedTime, compareBaseRecord);
        }
        for (String taskName : BenchPeriodicTask.TASK_NAME_LIST) {
            var compareBaseRecord = compareBaseMap.get(taskName);
            cerateOnlineTimeReport(config, sb, taskName, dedicatedTime, compareBaseRecord);
        }

        if (compareBaseKey != null) {
            sb.append("\n");
            sb.append("compare with ");
            sb.append(compareBaseKey);
            sb.append("\n\n");
        }

        return sb.toString();
    }

    private static void appendHeader(StringBuilder sb, List<String> headerList) {
        for (String header : headerList) {
            sb.append(header);
            sb.append("\n");
        }
    }

    private void createOnlineCounterReport(OnlineConfig config, StringBuilder sb, String taskName) {
        var tpm = getTpm(taskName);
        var counter = CostBenchDbManager.getCounter();

        var record = new OnlineCounterRecord(config, taskName, tpm, counter);
        sb.append(record);
        sb.append("\n");
    }

    private String getTpm(String taskName) {
        switch (taskName) {
        case BenchPeriodicUpdateStockTask.TASK_NAME:
            long interval = BenchConst.periodicInterval(taskName);
            return String.format("%.3f", (double) TimeUnit.MINUTES.toMillis(1) / interval);
        default:
            int tpm = BenchConst.onlineExecutePerMinute(taskName);
            return Integer.toString(tpm);
        }
    }

    private void cerateOnlineTimeReport(OnlineConfig config, StringBuilder sb, String taskName, long dedicatedTime, OnlineTimeRecord compareBaseRecord) {
        var counter = CostBenchDbManager.getCounter();

        var record = OnlineTimeRecord.of(config, taskName, dedicatedTime, counter, compareBaseRecord);
        sb.append(record);
        sb.append("\n");

        record.toCsv(csvReport);
        csvReport.append("\n");
    }

    // compare base

    private Map<OnlineAppReportHeader, Map<String, OnlineTimeRecord>> readCompareBaseFile(Path compareBaseFile) {
        List<String> lines;
        try {
            lines = Files.readAllLines(compareBaseFile);
        } catch (IOException e) {
            LOG.warn("read compareBaseFile error", e);
            return Map.of();
        }

        var allMap = new LinkedHashMap<OnlineAppReportHeader, Map<String, OnlineTimeRecord>>();

        OnlineAppReportHeader header = null;
        boolean onlineAppFile = false;
        for (String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            if (line.contains(HEADER1)) {
                onlineAppFile = true;
                continue;
            }
            if (!onlineAppFile) {
                continue;
            }
            if (line.startsWith("## ")) {
                header = OnlineAppReportHeader.parse(line.substring(2).trim());
                continue;
            }

            var record = OnlineTimeRecord.parse(line);
            if (record != null) {
                var map = allMap.computeIfAbsent(header, k -> new LinkedHashMap<>());
                map.put(record.taskName(), record);
            }
        }

        return allMap;
    }

    private OnlineAppReportHeader getCompareBaseKey(OnlineConfig config, OnlineAppReportHeader title, Map<OnlineAppReportHeader, Map<String, OnlineTimeRecord>> allMap) {
        if (allMap.isEmpty()) {
            return null;
        }

        List<OnlineAppReportHeader> keyList = new ArrayList<>(allMap.keySet());
        if (keyList.size() == 1) {
            return keyList.get(0);
        }

        { // coverRate
            int coverRate = config.getCoverRate();
            keyList = filterKey(keyList, it -> it.coverRate() == coverRate);
            if (keyList.size() == 1) {
                return keyList.get(0);
            }
        }
        { // onlineTxOption
            String onlineTxOption = config.getTxOption("online");
            keyList = filterKey(keyList, it -> it.onlineTxOption().equals(onlineTxOption));
            if (keyList.size() == 1) {
                return keyList.get(0);
            }
        }
        { // periodicTxOption
            String periodicTxOption = config.getTxOption("periodic");
            keyList = filterKey(keyList, it -> it.onlineTxOption().equals(periodicTxOption));
            if (keyList.size() == 1) {
                return keyList.get(0);
            }
        }

        LOG.warn("can't decide on one. {}", keyList);
        return keyList.get(0);
    }

    private List<OnlineAppReportHeader> filterKey(List<OnlineAppReportHeader> keyList, Predicate<OnlineAppReportHeader> predicate) {
        List<OnlineAppReportHeader> keys = keyList.stream().filter(predicate).collect(Collectors.toList());
        if (!keys.isEmpty()) {
            return keys;
        }
        return keyList;
    }
}
