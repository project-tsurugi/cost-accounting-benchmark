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

public class OnlineAppReport {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineAppReport.class);

    private static final String HEADER1 = "# Online Application Report";

    private final StringBuilder onlineAppReport = new StringBuilder(HEADER1 + "\n");

    public void writeOnlineAppReport(OnlineConfig config, OnlineAppReportHeader title, Path outputPath, long dedicatedTime, Path compareBaseFile) {
        LOG.debug("Creating an online application report for {}", title);

        String newReport = createOnlineAppReport(config, title, dedicatedTime, compareBaseFile);
        LOG.debug("Online application report: {}", newReport);
        this.onlineAppReport.append(newReport);

        Path outputFile = getOutputFile(outputPath);
        LOG.debug("Writing online application reports to {}", outputFile.toAbsolutePath());
        try {
            Files.writeString(outputFile, onlineAppReport);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    private Path getOutputFile(Path outputPath) {
        String fileName;
        {
            var outputFileName = outputPath.getFileName();
            if (outputFileName == null) {
                fileName = "online-app.md";
            } else {
                var batchFileName = outputFileName.toString();
                int n = batchFileName.lastIndexOf(".");
                if (n >= 0) {
                    fileName = batchFileName.substring(0, n) + ".online-app.md";
                } else {
                    fileName = batchFileName + ".online-app.md";
                }
            }
        }

        var dir = outputPath.getParent();
        if (dir != null) {
            return dir.resolve(fileName);
        } else {
            return Path.of(fileName);
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
