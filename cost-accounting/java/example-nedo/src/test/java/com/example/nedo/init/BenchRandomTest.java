package com.example.nedo.init;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

	@Test
	void testRandomExclude() {
		for (int i = 0; i < 10000; i++) {
			BigDecimal r = random.randomExclude(D_START, D_END);
			assertTrue(D_START.compareTo(r) < 0 && r.compareTo(D_END) < 0, () -> "r=" + r);
			assertEquals(1, r.scale());
		}
	}

	@Test
	void testRandom0() {
		for (int i = 0; i < 10000; i++) {
			BigDecimal r = random.random0(D_END);
			assertTrue(BigDecimal.ZERO.compareTo(r) <= 0 && r.compareTo(D_END) <= 0, () -> "r=" + r);
			assertEquals(1, r.scale());
		}
	}

	@Test
	void testSplit1() {
		BigDecimal[] rs = random.split(BigDecimal.TEN, 1);
		assertArrayEquals(new BigDecimal[] { BigDecimal.TEN }, rs);
	}

	@Test
	void testSplit_sizeLess() {
		BigDecimal value = new BigDecimal("1.0");
		BigDecimal[] rs = random.split(value, 20);
//		System.out.println(Arrays.toString(rs));
		assertEquals(20, rs.length);
		BigDecimal sum = BigDecimal.ZERO;
		for (BigDecimal r : rs) {
			assertTrue(BigDecimal.ZERO.compareTo(r) <= 0 && r.compareTo(value) <= 0, () -> "r=" + r);
			sum = sum.add(r);
		}
		assertEquals(value, sum);
	}

	@Test
	void testSplit_sizeEquals() {
		BigDecimal value = new BigDecimal("2.0");
		BigDecimal[] rs = random.split(value, 20);
//		System.out.println(Arrays.toString(rs));
		assertEquals(20, rs.length);
		BigDecimal sum = BigDecimal.ZERO;
		for (BigDecimal r : rs) {
			assertTrue(BigDecimal.ZERO.compareTo(r) <= 0 && r.compareTo(value) <= 0, () -> "r=" + r);
			sum = sum.add(r);
		}
		assertEquals(value, sum);
	}

	@Test
	void testSplit_sizeLarge() {
		BigDecimal value = new BigDecimal("100.0");
		BigDecimal[] rs = random.split(value, 20);
//		System.out.println(Arrays.toString(rs));
		assertEquals(20, rs.length);
		BigDecimal sum = BigDecimal.ZERO;
		for (BigDecimal r : rs) {
			assertTrue(BigDecimal.ZERO.compareTo(r) <= 0 && r.compareTo(value) <= 0, () -> "r=" + r);
			sum = sum.add(r);
		}
		assertEquals(value, sum);
	}
}
