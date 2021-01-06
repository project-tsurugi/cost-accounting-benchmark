package com.example.nedo.app.billing;

/**
 * 単純な月額料金計算クラス
 *
 */
public class SimpleMonthlyBillingCalculator implements MonthlyBillingCalculator {

	/**
	 * 基本料金3000円、基本料金に2000円分の無料通話分を含む場合の月額料金を計算する
	 */
	@Override
	public int calc(int totalCallCharge) {
		if (totalCallCharge < 0) {
			throw new IllegalArgumentException("negative chage: " + totalCallCharge);
		}
		if (totalCallCharge < 2000) {
			return 3000;
		}
		return totalCallCharge + 1000;
	}


}
