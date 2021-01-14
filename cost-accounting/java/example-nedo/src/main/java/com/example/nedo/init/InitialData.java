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

	protected <T extends HasDateRange> void initializePrevStartEndDate(int seed, T src, T dst) {
		LocalDate srcStartDate = src.getEffectiveDate();

		LocalDate endDate = srcStartDate.minusDays(1);
		dst.setExpiredDate(endDate);
		dst.setEffectiveDate(endDate.minusDays(random(seed, 1, 700)));
	}

	protected <T extends HasDateRange> void initializeNextStartEndDate(int seed, T src, T dst) {
		LocalDate srcEndDate = src.getExpiredDate();

		LocalDate startDate = srcEndDate.plusDays(1);
		dst.setEffectiveDate(startDate);
		dst.setExpiredDate(startDate.plusDays(random(seed, 1, 700)));
	}

	protected void weightToVolume(int weight, BigDecimal weighRatio) {
		// weight = volume * ratio
		// volume = weight / ratio
		throw new InternalError("yet");
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
