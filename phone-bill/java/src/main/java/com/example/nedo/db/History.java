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
	public Integer charge;

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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((caller_phone_number == null) ? 0 : caller_phone_number.hashCode());
		result = prime * result + ((charge == null) ? 0 : charge.hashCode());
		result = prime * result + (df ? 1231 : 1237);
		result = prime * result + ((payment_categorty == null) ? 0 : payment_categorty.hashCode());
		result = prime * result + ((recipient_phone_number == null) ? 0 : recipient_phone_number.hashCode());
		result = prime * result + ((start_time == null) ? 0 : start_time.hashCode());
		result = prime * result + time_secs;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		History other = (History) obj;
		if (caller_phone_number == null) {
			if (other.caller_phone_number != null)
				return false;
		} else if (!caller_phone_number.equals(other.caller_phone_number))
			return false;
		if (charge == null) {
			if (other.charge != null)
				return false;
		} else if (!charge.equals(other.charge))
			return false;
		if (df != other.df)
			return false;
		if (payment_categorty == null) {
			if (other.payment_categorty != null)
				return false;
		} else if (!payment_categorty.equals(other.payment_categorty))
			return false;
		if (recipient_phone_number == null) {
			if (other.recipient_phone_number != null)
				return false;
		} else if (!recipient_phone_number.equals(other.recipient_phone_number))
			return false;
		if (start_time == null) {
			if (other.start_time != null)
				return false;
		} else if (!start_time.equals(other.start_time))
			return false;
		if (time_secs != other.time_secs)
			return false;
		return true;
	}
}
