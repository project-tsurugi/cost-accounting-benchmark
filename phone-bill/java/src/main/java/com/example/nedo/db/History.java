package com.example.nedo.db;

import java.sql.Timestamp;

public class History {
	/**
	 * 発信者電話番号
	 */
	public String caller_phone_number;

	/**
	 * 受信者電話番号
	 */
	public String recipient_phone_number;

	/**
	 * 料金区分(発信者負担(C)、受信社負担(R))
	 */

	public String payment_categorty;
	 /**
	 * 通話開始時刻
	 */

	public Timestamp start_time;
	 /**
	 * 通話時間(秒)
	 */

	public int time_secs;

	/**
	 * 料金
	 */
	public int charge = 0;

	/**
	 * 削除フラグ
	 */
	public boolean df = false;

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("History [caller_phone_number=");
		builder.append(caller_phone_number);
		builder.append(", recipient_phone_number=");
		builder.append(recipient_phone_number);
		builder.append(", payment_categorty=");
		builder.append(payment_categorty);
		builder.append(", start_time=");
		builder.append(start_time);
		builder.append(", time_secs=");
		builder.append(time_secs);
		builder.append(", charge=");
		builder.append(charge);
		builder.append(", df=");
		builder.append(df);
		builder.append("]");
		return builder.toString();
	}
}
