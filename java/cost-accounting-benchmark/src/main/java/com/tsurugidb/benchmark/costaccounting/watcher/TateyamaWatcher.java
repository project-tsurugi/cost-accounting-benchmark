package com.tsurugidb.benchmark.costaccounting.watcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TateyamaWatcher implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TateyamaWatcher.class);

    private static final Path PROC = Path.of("/proc");

    private AtomicBoolean stopRequested = new AtomicBoolean(false);

    private final int pid;
    private long vsz = -1;
    private long rss = -1;

    public TateyamaWatcher(int pid) {
        this.pid = pid;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("TateyamaWatcher");
        final NumberFormat fmt = NumberFormat.getNumberInstance(Locale.US);

        if (pid == -1) {
            LOG.error("Unable to locate tateyama-server. Terminating TateyamaWatcher.");
            return;
        }
        Path path = PROC.resolve(Integer.toString(pid)).resolve("status");

        // 1秒に1回サーバのメモリ容量を出力する
        for (;;) {
            try {
                for (String line : Files.readAllLines(path)) {
                    if (line.startsWith("VmSize:")) {
                        vsz = parseMemoryValue(line);
                    } else if (line.startsWith("VmRSS:")) {
                        rss = parseMemoryValue(line);
                    }
                }
            } catch (IOException e) {
                String msg = "Unable to retrieve memory info for tateyama-server. It is possible that the server has crashed.";
                LOG.error(msg, e);
                throw new UncheckedIOException(e);
            }
            LOG.debug("tateyama-server memory info: VSZ = {} bytes, RSS = {} bytes", fmt.format(vsz), fmt.format(rss));
            if (stopRequested.get()) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void stop() {
        stopRequested.set(true);
    }

    /**
     * プロセスステータスの行からメモリサイズを抽出するメソッド
     *
     * @param line プロセスステータスの行
     * @return メモリサイズ（バイト単位）
     */
    static long parseMemoryValue(String line) {
        long value = -1;
        String[] parts = line.split("\\s+");
        if (parts.length == 2) {
            try {
                value = Long.parseLong(parts[1]);
            } catch (NumberFormatException e) {
                // ignore
            }
        } else if (parts.length == 3 && parts[2].equals("kB")) {
            try {
                value = Long.parseLong(parts[1]) * 1024;
            } catch (NumberFormatException e) {
                // ignore
            }
        } else if (parts.length == 3 && parts[2].equals("mB")) {
            try {
                value = Long.parseLong(parts[1]) * 1024 * 1024;
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return value;
    }

    /**
     * @return vsz
     */
    public long getVsz() {
        return vsz;
    }

    /**
     * @return rss
     */
    public long getRss() {
        return rss;
    }
}
