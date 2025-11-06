/*
 * Copyright 2023-2025 Project Tsurugi.
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
import java.util.Arrays;
import java.util.TreeSet;

public class BenchReproducibleRandom {

    public BigDecimal randomExclude(int seed, BigDecimal start, BigDecimal end) {
        for (;; seed++) {
            BigDecimal r = prandom(seed, start, end);
            if (r.compareTo(start) == 0 || r.compareTo(end) == 0) {
                continue;
            }
            return r;
        }
    }

    /**
     * 0に近い乱数
     */
    public BigDecimal random0(int seed, BigDecimal end) {
        BigDecimal start = BigDecimal.ZERO;
        BigDecimal r = prandom(seed, start, end);
        BigDecimal s = prandom(seed + 1, start, end);
        BigDecimal r2 = end.subtract(r);
        if (s.compareTo(r2) <= 0) {
            return r;
        } else {
            return r2;
        }
    }

    public BigDecimal[] split(int seed, BigDecimal value, int splitSize) {
        assert splitSize > 0;

        if (splitSize == 1) {
            return new BigDecimal[] { value };
        }

        int valueSize = getSize(value);
        if (valueSize <= splitSize) {
            return splitUsingUlp(seed, value, valueSize, splitSize);
        }

        return splitUsingSet(seed, value, splitSize);
    }

    private int getSize(BigDecimal value) {
        int scale = value.scale();
        long v = value.movePointRight(scale).longValue();
        return (int) v;
    }

    private BigDecimal[] splitUsingUlp(int seed, BigDecimal value, int valueSize, int splitSize) {
        BigDecimal ulp = value.ulp();

        BigDecimal[] result = new BigDecimal[splitSize];
        Arrays.fill(result, BigDecimal.ZERO);
        for (int i = 0; i < valueSize; i++) {
            int n = prandom(seed + i, splitSize);
            result[n] = result[n].add(ulp);
        }
        return result;
    }

    private BigDecimal[] splitUsingSet(int seed, BigDecimal value, int splitSize) {
        TreeSet<BigDecimal> set = new TreeSet<>();
        while (set.size() < splitSize - 1) {
            BigDecimal r = randomExclude(seed++, BigDecimal.ZERO, value);
            set.add(r);
        }
        assert set.size() == splitSize - 1;

        BigDecimal[] result = new BigDecimal[splitSize];
        int i = 0;
        BigDecimal prev = BigDecimal.ZERO;
        for (BigDecimal v : set) {
            result[i++] = v.subtract(prev);
            prev = v;
        }
        result[i] = value.subtract(prev);

        return result;
    }

    public int prandom(int seed, int size) {
        double r = (Math.sin(seed) + 1) / 2;
        int n = (int) (r * size);
        if (n >= size) {
            return size - 1;
        }
        return n;
    }

    public int prandom(int seed, int start, int end) {
        assert start <= end;

        int size = end - start + 1;
        return prandom(seed, size) + start;
    }

    private long prandom(int seed, long size) {
        double r = (Math.sin(seed) + 1) / 2;
        long n = (long) (r * size);
        if (n >= size) {
            return size - 1;
        }
        return n;
    }

    public BigDecimal prandom(int seed, BigDecimal start, BigDecimal end) {
        int scale = Math.max(start.scale(), end.scale());
        long s = start.movePointRight(scale).longValue();
        long e = end.movePointRight(scale).longValue();
        long size = e - s + 1;
        long r = prandom(seed, size) + s;
        return BigDecimal.valueOf(r).movePointLeft(scale);
    }

    public BigDecimal prandomExclude(int seed, BigDecimal start, BigDecimal end) {
        for (;;) {
            BigDecimal r = prandom(seed, start, end);
            if (r.compareTo(start) == 0 || r.compareTo(end) == 0) {
                continue;
            }
            return r;
        }
    }

    public BigDecimal[] psplit(int seed, BigDecimal value, int splitSize) {
        assert splitSize > 0;

        if (splitSize == 1) {
            return new BigDecimal[] { value };
        }

        int valueSize = getSize(value);
        if (valueSize <= splitSize) {
            return psplitUsingUlp(seed, value, valueSize, splitSize);
        }

        return psplitUsingSet(seed, value, splitSize);
    }

    private BigDecimal[] psplitUsingUlp(int seed, BigDecimal value, int valueSize, int splitSize) {
        BigDecimal ulp = value.ulp();

        BigDecimal[] result = new BigDecimal[splitSize];
        Arrays.fill(result, BigDecimal.ZERO);
        for (int i = 0; i < valueSize; i++) {
            int n = prandom(seed + i, splitSize);
            result[n] = result[n].add(ulp);
        }
        return result;
    }

    private BigDecimal[] psplitUsingSet(int seed, BigDecimal value, int splitSize) {
        TreeSet<BigDecimal> set = new TreeSet<>();
        while (set.size() < splitSize - 1) {
            BigDecimal r = prandomExclude(seed++, BigDecimal.ZERO, value);
            set.add(r);
        }
        assert set.size() == splitSize - 1;

        BigDecimal[] result = new BigDecimal[splitSize];
        int i = 0;
        BigDecimal prev = BigDecimal.ZERO;
        for (BigDecimal v : set) {
            result[i++] = v.subtract(prev);
            prev = v;
        }
        result[i] = value.subtract(prev);

        return result;
    }
}
