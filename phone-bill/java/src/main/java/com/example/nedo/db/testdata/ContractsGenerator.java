package com.example.nedo.db.testdata;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author umega
 *
 */
public class ContractsGenerator {



	/**
	 * 11桁の電話番号をLONG値で表したときの最大値
	 */
	private static final long MAX_PHNE_NUMBER = 99999999999L;

	/**
	 * 一度にコミットする行数
	 */
	private static final long RECORDS_AT_COMMIT = 3000;

	/**
	 * ミリ秒で表した1日
	 */
	private static final long A_DAY_IN_MILLISECONDS = 24 * 3600 * 1000;

	private List<Duration> durationList = new ArrayList<>();



	private Random random;
	private int numberOfRecords;
	private int duplicatePhoneNumberRatio;
	private int expirationDateRate;
	private int noExpirationDateRate;
	private Date minDate;
	private Date maxDate;


	/**
	 * テストデータ生成のためのパラメータを指定してContractsGeneratorのインスタンスを生成する.
	 *
	 *
	 * @param seed 乱数のシード
	 * @param numberOfRecords 作成するレコード数
	 * @param duplicatePhoneNumberRatio 電話番号が重複する割合
	 * @param expirationDateRate 契約終了日がある電話番号の割合
	 * @param noExpirationDateRate 契約終了日がない電話番号の割合
	 * @param minDate 契約開始日の最小値
	 * @param maxDate 契約終了日の最大値
	 */
	public ContractsGenerator(long seed, int numberOfRecords, int duplicatePhoneNumberRatio,
			int expirationDateRate, int noExpirationDateRate, Date minDate, Date maxDate) {
		if (minDate.getTime() >= maxDate.getTime()) {
			new RuntimeException(
					"maxDate is less than or equal to minDate, minDate =" + minDate + ", maxDate = " + maxDate);
		}

		this.random = new Random(seed);
		this.numberOfRecords = numberOfRecords;
		this.duplicatePhoneNumberRatio = duplicatePhoneNumberRatio;
		this.expirationDateRate =expirationDateRate;
		this.noExpirationDateRate = noExpirationDateRate;
		this.minDate = minDate;
		this.maxDate = maxDate;
		initDurationList();
	}






	/**
	 * 契約日のパターンのリストを作成する
	 */
	private void initDurationList() {
		// TODO: もっとバリエーションが欲しい
		// 契約終了日がないduration
		for(int i = 0; i <noExpirationDateRate; i++) {
			Date start = getDate(minDate, maxDate);
			durationList.add(new Duration(start, null));
		}
		// 契約終了日があるduration
		for(int i = 0; i <expirationDateRate; i++) {
			Date start = getDate(minDate, maxDate);
			Date end = getDate(start, maxDate);
			durationList.add(new Duration(start, end));
		}
		// 同一電話番号の契約が複数あるパターン用のduration
		for(int i = 0; i <duplicatePhoneNumberRatio; i++) {
			Date end = getDate(minDate, maxDate);
			Date start = getDate(end, maxDate);;
			durationList.add(new Duration(minDate, end));
			durationList.add(new Duration(nextDate(start), null));
		}
	}


	/**
	 * 指定のdateの次の日を返す
	 *
	 * @param date
	 * @return
	 */
	Date nextDate(Date date) {
		return new Date(date.getTime() + A_DAY_IN_MILLISECONDS);
	}


	/**
	 * durationListを取得する(UT用)
	 *
	 * @return durationList
	 */
	List<Duration> getDurationList() {
		return durationList;
	}


	/**
	 * min～maxの範囲のランダムな日付を取得する
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	Date getDate(Date min, Date max) {
		int days = (int) ((max.getTime() - min.getTime()) / A_DAY_IN_MILLISECONDS);
		long offset = random.nextInt(days + 1) * A_DAY_IN_MILLISECONDS;
		return new Date(min.getTime() + offset);
	}



	/**
	 * n番目のレコードの電話番号(11桁)を返す
	 *
	 * @param n
	 * @return
	 */
	String getPhoneNumber(long  n) {
		if (n < 0 || MAX_PHNE_NUMBER <= n) {
			throw new RuntimeException("Out of phone number range: " + n);
		}
		// TODO 電話番号が連番にならないようにする
		long blockSize = duplicatePhoneNumberRatio + expirationDateRate + noExpirationDateRate;
		long noDupSize = expirationDateRate + noExpirationDateRate;
		long posInBlock = n % blockSize;
		long phoneNumber = n;
		if (posInBlock >= noDupSize && posInBlock % 2 == 0) {
			phoneNumber = n + 1;
		}
		String format = "%011d";
		return  String.format(format, phoneNumber);
	}


	/**
	 * n番目のレコードのDurationを返す
	 *
	 * @param n
	 * @return
	 */
	Duration getDuration(long  n) {
		return  durationList.get((int) (n % durationList.size()));
	}


	/**
	 * 契約期間を表すクラス
	 *
	 */
	public class Duration {
		Date start;
		Date end;
		public Duration(Date start, Date end) {
			this.start = start;
			this.end = end;
		}
	}
}
