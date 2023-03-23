package com.tsurugidb.benchmark.costaccounting.watcher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class TateyamaWatcherService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TateyamaWatcherService.class);

    private static final String SERVER_NAME = "libexec/tateyama-server";
    private static final Path PROC = Path.of("/proc");

    private final TateyamaWatcher task;
    private Future<?> future;

    public static TateyamaWatcherService of() {
        TateyamaWatcher task = null;
        if (BenchConst.dbmsType() == DbmsType.TSURUGI) {
            int pid = findServerPid();
            task = new TateyamaWatcher(pid);
        }
        return new TateyamaWatcherService(task);
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
                    LOG.info("Found tateyama-server pid = {}, bad owner is not {}", pid, me);
                } else {
                    LOG.info("Found tateyama-server pid = {}", pid);
                    return pid;
                }
            } catch (IOException e) {
                // cmdlineを読み取れない => 他のユーザのプロセス or 存在しないプロセス => 無視する
                continue;
            }
        }
        return -1;
    }

    protected TateyamaWatcherService(TateyamaWatcher task) {
        this.task = task;
    }

    public TateyamaWatcher start() {
        if (this.task != null) {
            var service = Executors.newFixedThreadPool(1);
            this.future = service.submit(task);
        }

        return this.task;
    }

    @Override
    public void close() throws InterruptedException, ExecutionException {
        if (this.task != null) {
            task.stop();
            future.get();

            double m = 1024d * 1024 * 1024; // GB
            var vsz = String.format("%.1f", task.getVsz() / m);
            var rss = String.format("%.1f", task.getRss() / m);
            LOG.info("tateyama-server memory info: VSZ = {} GB, RSS = {} GB", vsz, rss);
        }
    }
}
