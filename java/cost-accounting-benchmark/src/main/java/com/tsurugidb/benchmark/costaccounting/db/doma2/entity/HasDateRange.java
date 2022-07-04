package com.tsurugidb.benchmark.costaccounting.db.doma2.entity;

import java.time.LocalDate;

public interface HasDateRange {

    public LocalDate getEffectiveDate();

    public void setEffectiveDate(LocalDate date);

    public LocalDate getExpiredDate();

    public void setExpiredDate(LocalDate date);
}
