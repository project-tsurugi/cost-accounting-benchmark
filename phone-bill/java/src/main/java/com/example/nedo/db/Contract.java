package com.example.nedo.db;

import java.sql.Date;

public class Contract {
	/**
	 * 電話番号
	 */
	public String phoneNumber;

	/**
	 * 契約開始日
	 */
	public Date startDate;

	/**
	 * 契約終了日
	 */
	public Date endDate;

	/**
	 * 料金計算ルール
	 */
	public String rule;

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Contract [phone_number=");
		builder.append(phoneNumber);
		builder.append(", start_date=");
		builder.append(startDate);
		builder.append(", end_date=");
		builder.append(endDate);
		builder.append(", rule=");
		builder.append(rule);
		builder.append("]");
		return builder.toString();
	}
}
