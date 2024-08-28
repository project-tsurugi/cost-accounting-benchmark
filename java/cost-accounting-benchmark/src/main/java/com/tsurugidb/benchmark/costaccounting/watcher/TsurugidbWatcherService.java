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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class TsurugidbWatcherService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugidbWatcherService.class);

    private static final String SERVER_NAME = "libexec/tsurugidb";
    private static final Path PROC = Path.of("/proc");

    private final TsurugidbWatcher task;
    private ExecutorService service;
    private Future<?> future;

    public static TsurugidbWatcherService of(boolean enableTsurugidbWatcher) {
        TsurugidbWatcher task = null;
        if (BenchConst.dbmsType() == DbmsType.TSURUGI && enableTsurugidbWatcher) {
            int pid = findServerPid();
            task = new TsurugidbWatcher(pid);
        }
        return new TsurugidbWatcherService(task);
    }

    /**
     * /procを調べてサーバのプロセスIDを取得する
     *
     * @return サーバのプロセスID、見つからなかったときは -1
     */
    private static int findServerPid() {
        String me = System.getProperty("user.name");

        List<Path> procDirs = null;
        try {
            if (!Files.exists(PROC)) {
                LOG.debug("Directory not found: {}", PROC.toString());
                return -1;
            }
            procDirs = Files.list(PROC).filter(p -> Files.isDirectory(p)).collect(Collectors.toList());
            LOG.debug("{} directories found in {}", procDirs.size(), PROC.toString());
        } catch (IOException e) {
            LOG.warn("IOError", e);
            return -1;
        }
        for (Path dir : procDirs) {
            Path cmdline = dir.resolve("cmdline");
            LOG.trace("Checking direcory: {}", dir.toAbsolutePath().toString());
            try {
                if (!Files.readString(cmdline).contains(SERVER_NAME)) {
                    continue;
                }
                Path p = dir.getFileName();
                int pid = p == null ? -1 : Integer.parseInt(p.toString());
                if (pid == -1) {
                    continue;
                }
                UserPrincipal owner = Files.getOwner(cmdline);
                if (!owner.getName().equals(me)) {
                    LOG.info("Found tsurugidb pid = {}, bad owner is not {}", pid, me);
                } else {
                    LOG.info("Found tsurugidb pid = {}", pid);
                    return pid;
                }
            } catch (IOException e) {
                // cmdlineを読み取れない => 他のユーザのプロセス or 存在しないプロセス => 無視する
                continue;
            }
        }
        return -1;
    }

    protected TsurugidbWatcherService(TsurugidbWatcher task) {
        this.task = task;
    }

    public TsurugidbWatcher start() {
        if (this.task != null) {
            this.service = Executors.newFixedThreadPool(1);
            this.future = service.submit(task);
            LOG.info("TsurugidbWatcher started");
        }

        return this.task;
    }

    @Override
    public void close() throws Exception {
        if (this.task != null) {
            LOG.info("TsurugidbWatcher close start");
            try (AutoCloseable c = () -> service.shutdownNow()) {
                task.stop();
                future.get();

                double m = 1024d * 1024 * 1024; // GB
                var vsz = String.format("%.1f", task.getVsz() / m);
                var rss = String.format("%.1f", task.getRss() / m);
                LOG.info("tsurugidb memory info: VSZ = {} GB, RSS = {} GB", vsz, rss);
            }
            LOG.info("TsurugidbWatcher close end");
        }
    }
}
