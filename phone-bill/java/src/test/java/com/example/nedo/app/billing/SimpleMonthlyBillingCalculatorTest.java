package com.example.nedo.app.billing;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SimpleMonthlyBillingCalculatorTest {

	@Test
	void test() {
		SimpleMonthlyBillingCalculator calculator = new SimpleMonthlyBillingCalculator();

		assertEquals(3000, calculator.calc(0));
		assertEquals(3000, calculator.calc(1));
		assertEquals(3000, calculator.calc(1999));
		assertEquals(3000, calculator.calc(2000));
		assertEquals(3001, calculator.calc(2001));
		assertEquals(7001, calculator.calc(6001));
		assertThrows(IllegalArgumentException.class, () -> calculator.calc(-1));
	}

}
