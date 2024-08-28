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

public class TsurugidbWatcher implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugidbWatcher.class);

    private static final Path PROC = Path.of("/proc");

    private AtomicBoolean stopRequested = new AtomicBoolean(false);

    private final int pid;
    private long vsz = -1;
    private long rss = -1;

    public TsurugidbWatcher(int pid) {
        this.pid = pid;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("TsurugidbWatcher");
        final NumberFormat fmt = NumberFormat.getNumberInstance(Locale.US);

        if (pid == -1) {
            LOG.error("Unable to locate tsurugidb. Terminating TateyamaWatcher.");
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
                String msg = String.format( //
                        "Failed to access the process status file for tsurugidb at %s: " + //
                                "The server might be shut down. Check server status and logs.", //
                        path.toString()); //
                LOG.error(msg, e);
                throw new UncheckedIOException(e);
            }
            LOG.debug("tsurugidb memory info: VSZ = {} bytes, RSS = {} bytes", fmt.format(vsz), fmt.format(rss));
            if (stopRequested.get()) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error("TsurugidbWatcher thread interrupted", e);
                throw new RuntimeException(e);
            }
        }
        LOG.info("TsurugidbWatcher thread end");
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
