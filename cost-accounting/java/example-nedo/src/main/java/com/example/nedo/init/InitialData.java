package com.example.nedo.init;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import com.example.nedo.jdbc.doma2.entity.HasDateRange;

public class InitialData {
	public static final LocalDate DEFAULT_BATCH_DATE = LocalDate.of(2020, 9, 15);

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
		System.out.println("end " + startTime.until(endTime, ChronoUnit.SECONDS));
	}

	protected void initializeStartEndDate(HasDateRange entity) {
		LocalDate startDate = batchDate.minusDays(random(0, 700));
		entity.setEffectiveDate(startDate);

		LocalDate endDate = getRandomExpiredDate(batchDate);
		entity.setExpiredDate(endDate);
	}

	public LocalDate getRandomExpiredDate(LocalDate batchDate) {
		LocalDate endDate = batchDate.plusDays(random(7, 700));
		return endDate;
	}

	protected <T extends HasDateRange> void initializePrevStartEndDate(T src, T dst) {
		LocalDate srcStartDate = src.getEffectiveDate();

		LocalDate endDate = srcStartDate.minusDays(1);
		dst.setExpiredDate(endDate);
		dst.setEffectiveDate(endDate.minusDays(random(1, 700)));
	}

	protected <T extends HasDateRange> void initializeNextStartEndDate(T src, T dst) {
		LocalDate srcEndDate = src.getExpiredDate();

		LocalDate startDate = srcEndDate.plusDays(1);
		dst.setEffectiveDate(startDate);
		dst.setExpiredDate(startDate.plusDays(random(1, 700)));
	}

	protected void weightToVolume(int weight, BigDecimal weighRatio) {
		// weight = volume * ratio
		// volume = weight / ratio
		throw new InternalError("yet");
	}

	// random

	protected int random(int start, int end) {
		return random.random(start, end);
	}

	protected BigDecimal random(BigDecimal start, BigDecimal end) {
		return random.random(start, end);
	}

	public <T> T getRandomAndRemove(List<T> list) {
		assert list.size() > 0;

		int i = random.nextInt(list.size());
		return list.remove(i);
	}

}