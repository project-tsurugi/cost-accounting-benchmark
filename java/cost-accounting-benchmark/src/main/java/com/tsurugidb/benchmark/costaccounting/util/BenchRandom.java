package com.tsurugidb.benchmark.costaccounting.util;

import java.math.BigDecimal;
import java.util.Random;

public class BenchRandom {

    private Random random = new Random();

    public Random getRawRandom() {
        return this.random;
    }

    public int nextInt(int size) {
        return random.nextInt(size);
    }

    public int random(int start, int end) {
        assert start <= end;

        int size = end - start + 1;
        return random.nextInt(size) + start;
    }

    public BigDecimal random(BigDecimal start, BigDecimal end) {
        int scale = Math.max(start.scale(), end.scale());
        long s = start.movePointRight(scale).longValue();
        long e = end.movePointRight(scale).longValue();
        long size = e - s + 1;
        long r = nextLong(size) + s;
        return BigDecimal.valueOf(r).movePointLeft(scale);
    }

    private long nextLong(long bound) {
        long r = random.nextLong();
        long m = bound - 1;
        for (long u = r; u - (r = u % bound) + m < 0; u = random.nextLong())
            ;
        return r;
    }
}
