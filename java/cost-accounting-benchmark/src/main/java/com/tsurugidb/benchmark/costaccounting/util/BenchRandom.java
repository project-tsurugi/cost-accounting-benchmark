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
