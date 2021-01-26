package com.example.nedo.init;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import com.example.nedo.BenchConst;
import com.example.nedo.jdbc.doma2.entity.HasDateRange;

public class InitialData {
	public static final LocalDate DEFAULT_BATCH_DATE = BenchConst.initBatchDate();

	protected final LocalDate batchDate;

	private LocalDateTime startTime;

	protected final BenchRandom random = new BenchRandom();

	protected InitialData(LocalDate batchDate) {
		this.batchDate = batchDate;
	}

	protected void logStart() {
		startTime = LocalDateTime.now();
		System.out.println("start " + startTime);
	}

	protected void logEnd() {
		LocalDateTime endTime = LocalDateTime.now();
		System.out.println("end " + startTime.until(endTime, ChronoUnit.SECONDS) + "[s]");
	}

	protected void initializeStartEndDate(int seed, HasDateRange entity) {
		LocalDate startDate = batchDate.minusDays(random(seed, 0, 700));
		entity.setEffectiveDate(startDate);

		LocalDate endDate = getRandomExpiredDate(seed + 1, batchDate);
		entity.setExpiredDate(endDate);
	}

	public LocalDate getRandomExpiredDate(int seed, LocalDate batchDate) {
		LocalDate endDate = batchDate.plusDays(random(seed, 7, 700));
		return endDate;
	}

	// random

	protected int random(int seed, int start, int end) {
		return random.prandom(seed, start, end);
	}

	protected BigDecimal random(int seed, BigDecimal start, BigDecimal end) {
		return random.prandom(seed, start, end);
	}

	public <T> T getRandomAndRemove(int seed, List<T> list) {
		assert list.size() > 0;

		int i = random.prandom(seed, list.size());
		return list.remove(i);
	}

}
