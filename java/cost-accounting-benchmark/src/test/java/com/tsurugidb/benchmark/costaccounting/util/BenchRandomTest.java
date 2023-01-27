package com.tsurugidb.benchmark.costaccounting.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

class BenchRandomTest {

    private final BenchRandom random = new BenchRandom();

    @Test
    void testRandomIntInt() {
        for (int i = 0; i < 10000; i++) {
            int r = random.random(1, 10);
            assertTrue(1 <= r && r <= 10, () -> "r=" + r);
        }
    }

    private static final BigDecimal D_START = new BigDecimal("1.0");
    private static final BigDecimal D_END = new BigDecimal("10.0");

    @Test
    void testRandomBigDecimalBigDecimal() {
        for (int i = 0; i < 10000; i++) {
            BigDecimal r = random.random(D_START, D_END);
            assertTrue(D_START.compareTo(r) <= 0 && r.compareTo(D_END) <= 0, () -> "r=" + r);
            assertEquals(1, r.scale());
        }
    }
}
