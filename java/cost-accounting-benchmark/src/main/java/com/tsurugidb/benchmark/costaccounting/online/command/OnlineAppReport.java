package com.tsurugidb.benchmark.costaccounting.online.command;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    private final StringBuilder onlineAppReport = new StringBuilder("# Online Application Report \n");

    public void writeOnlineAppReport(OnlineConfig config, String title, Path outputPath, long dedicatedTime) {
        LOG.debug("Creating an online application report for {}", title);

        String newReport = createOnlineAppReport(config, title, dedicatedTime);
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

    private String createOnlineAppReport(OnlineConfig config, String title, long dedicatedTime) {
        var sb = new StringBuilder(2048);
        sb.append("\n");

        // タイトル
        sb.append("## ");
        sb.append(title);
        sb.append("\n\n");

        // counter
        appendHeader(sb, OnlineCounberRecord.HEADER_LIST);
        for (String taskName : BenchOnlineTask.TASK_NAME_LIST) {
            createOnlineCounterReport(config, sb, taskName);
        }
        for (String taskName : BenchPeriodicTask.TASK_NAME_LIST) {
            createOnlineCounterReport(config, sb, taskName);
        }

        sb.append("\n");

        // time
        appendHeader(sb, OnlineTimeRecord.HEADER_LIST);
        for (String taskName : BenchOnlineTask.TASK_NAME_LIST) {
            cerateOnlineTimeReport(config, sb, taskName, dedicatedTime);
        }
        for (String taskName : BenchPeriodicTask.TASK_NAME_LIST) {
            cerateOnlineTimeReport(config, sb, taskName, dedicatedTime);
        }

        return sb.toString();
    }

    private void appendHeader(StringBuilder sb, List<String> list) {
        for (String header : list) {
            sb.append(header);
            sb.append("\n");
        }
    }

    private void createOnlineCounterReport(OnlineConfig config, StringBuilder sb, String taskName) {
        int threads = config.getThreadSize(taskName);
        var tpm = getTpm(taskName);
        var counter = CostBenchDbManager.getCounter();

        var record = new OnlineCounberRecord(taskName, threads, tpm, counter);
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

    private void cerateOnlineTimeReport(OnlineConfig config, StringBuilder sb, String taskName, long dedicatedTime) {
        var counter = CostBenchDbManager.getCounter();

        var record = new OnlineTimeRecord(taskName, dedicatedTime, counter);
        sb.append(record);
        sb.append("\n");
    }
}
