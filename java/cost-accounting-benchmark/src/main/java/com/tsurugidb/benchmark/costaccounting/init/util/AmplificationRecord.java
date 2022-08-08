package com.tsurugidb.benchmark.costaccounting.init.util;

import java.time.LocalDate;
import java.util.Collection;
import java.util.TreeMap;

import com.tsurugidb.benchmark.costaccounting.db.entity.HasDateRange;
import com.tsurugidb.benchmark.costaccounting.util.BenchRandom;

// thread safe
public abstract class AmplificationRecord<T extends HasDateRange> {

    private final AmplificationSize amplificationSize;
    protected final BenchRandom random;

    public AmplificationRecord(double magnification, BenchRandom random) {
        this.amplificationSize = new AmplificationSize(magnification);
        this.random = random;
    }

    private final ThreadLocal<TreeMap<LocalDate, T>> mapPool = new ThreadLocal<TreeMap<LocalDate, T>>() {
        @Override
        protected TreeMap<LocalDate, T> initialValue() {
            return new TreeMap<>();
        }

        @Override
        public TreeMap<LocalDate, T> get() {
            TreeMap<LocalDate, T> r = super.get();
            r.clear();
            return r;
        }
    };

    public Collection<T> amplify(T entity) {
//		TreeMap<LocalDate, T> map = new TreeMap<>();
        TreeMap<LocalDate, T> map = mapPool.get();
        map.put(entity.getEffectiveDate(), entity);

        final int size = amplificationSize.amplificationSize(getAmplificationId(entity));
        for (int i = 0; i < size; i++) {
            T ent;
            int seed = getSeed(entity) + i;
            if (random.prandom(seed, 0, 1) == 0) {
                T src = map.firstEntry().getValue();
                ent = getClone(src);
                initializePrevStartEndDate(seed + 1, src, ent);
            } else {
                T src = map.lastEntry().getValue();
                ent = getClone(src);
                initializeNextStartEndDate(seed + 1, src, ent);
            }

            initialize(ent);
            map.put(ent.getEffectiveDate(), ent);
        }

        return map.values();
    }

    protected abstract int getAmplificationId(T entity);

    protected abstract int getSeed(T entity);

    protected abstract T getClone(T entity);

    private void initializePrevStartEndDate(int seed, T src, T dst) {
        LocalDate srcStartDate = src.getEffectiveDate();

        LocalDate endDate = srcStartDate.minusDays(1);
        dst.setExpiredDate(endDate);
        dst.setEffectiveDate(endDate.minusDays(random.prandom(seed, 1, 700)));
    }

    private void initializeNextStartEndDate(int seed, T src, T dst) {
        LocalDate srcEndDate = src.getExpiredDate();

        LocalDate startDate = srcEndDate.plusDays(1);
        dst.setEffectiveDate(startDate);
        dst.setExpiredDate(startDate.plusDays(random.prandom(seed, 1, 700)));
    }

    protected abstract void initialize(T entity);
}
