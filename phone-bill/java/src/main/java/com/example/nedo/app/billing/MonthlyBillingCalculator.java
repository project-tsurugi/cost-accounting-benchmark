package com.example.nedo.app.billing;

public interface MonthlyBillingCalculator {
	/**
	 * 通話料金の総額から、請求額を計算する
	 *
	 * @param totalCaharge 通話料金の総額
	 * @return 請求額
	 */
	int calc(int totalCallCharge);
}
