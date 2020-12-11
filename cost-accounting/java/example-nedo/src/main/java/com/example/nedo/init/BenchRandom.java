package com.example.nedo.init;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Random;
import java.util.TreeSet;

public class BenchRandom {

	private Random random = new Random();

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

	public BigDecimal randomExclude(BigDecimal start, BigDecimal end) {
		for (;;) {
			BigDecimal r = random(start, end);
			if (r.compareTo(start) == 0 || r.compareTo(end) == 0) {
				continue;
			}
			return r;
		}
	}

	public BigDecimal random0(BigDecimal end) {
		BigDecimal start = BigDecimal.ZERO;
		BigDecimal r = random(start, end);
		BigDecimal s = random(start, end);
		BigDecimal r2 = end.subtract(r);
		if (s.compareTo(r2) <= 0) {
			return r;
		} else {
			return r2;
		}
	}

	public BigDecimal[] split(BigDecimal value, int splitSize) {
		assert splitSize > 0;

		if (splitSize == 1) {
			return new BigDecimal[] { value };
		}

		int valueSize = getSize(value);
		if (valueSize <= splitSize) {
			return splitUsingUlp(value, valueSize, splitSize);
		}

		return splitUsingSet(value, splitSize);
	}

	private int getSize(BigDecimal value) {
		int scale = value.scale();
		long v = value.movePointRight(scale).longValue();
		return (int) v;
	}

	private BigDecimal[] splitUsingUlp(BigDecimal value, int valueSize, int splitSize) {
		BigDecimal ulp = value.ulp();

		BigDecimal[] result = new BigDecimal[splitSize];
		Arrays.fill(result, BigDecimal.ZERO);
		for (int i = 0; i < valueSize; i++) {
			int n = random.nextInt(splitSize);
			result[n] = result[n].add(ulp);
		}
		return result;
	}

	private BigDecimal[] splitUsingSet(BigDecimal value, int splitSize) {
		TreeSet<BigDecimal> set = new TreeSet<>();
		while (set.size() < splitSize - 1) {
			BigDecimal r = randomExclude(BigDecimal.ZERO, value);
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