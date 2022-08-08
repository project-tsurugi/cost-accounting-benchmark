package com.tsurugidb.benchmark.costaccounting.db.entity;

import java.time.LocalDate;

public interface HasDateRange {

    public LocalDate getEffectiveDate();

    public void setEffectiveDate(LocalDate date);

    public LocalDate getExpiredDate();

    public void setExpiredDate(LocalDate date);
}
