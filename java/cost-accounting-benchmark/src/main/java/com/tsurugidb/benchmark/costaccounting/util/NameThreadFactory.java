package com.tsurugidb.benchmark.costaccounting.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NameThreadFactory implements ThreadFactory {

    private final String baseName;

    private final AtomicInteger number = new AtomicInteger(0);

    public NameThreadFactory(String baseName) {
        this.baseName = baseName;
    }

    @Override
    public Thread newThread(Runnable r) {
        var thread = new Thread(r);
        thread.setName(baseName + number.getAndIncrement());
        return thread;
    }
}
