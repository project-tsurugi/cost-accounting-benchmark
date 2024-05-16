package com.tsurugidb.benchmark.costaccounting.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadUtil {

    public static ExecutorService newFixedThreadPool(String baseName, int threads) {
        var factory = new NameThreadFactory(baseName);
        return Executors.newFixedThreadPool(threads, factory);
    }
}
