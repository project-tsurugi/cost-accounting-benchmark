package com.tsurugidb.benchmark.costaccounting.db.entity;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Objects;

/**
 * key(s_date, s_time) for stock_history
 */
public class StockHistoryDateTime implements Comparable<StockHistoryDateTime> {

    /** s_date date(8) */
    private LocalDate sDate;

    /** s_time time(6) */
    private LocalTime sTime;

    public void setSDate(LocalDate value) {
        this.sDate = value;
    }

    public LocalDate getSDate() {
        return this.sDate;
    }

    public void setSTime(LocalTime value) {
        this.sTime = value;
    }

    public LocalTime getSTime() {
        return this.sTime;
    }

    @Override
    public int compareTo(StockHistoryDateTime o) {
        int c = sDate.compareTo(o.getSDate());
        if (c != 0) {
            return c;
        }
        return sTime.compareTo(o.getSTime());
    }

    @Override
    public int hashCode() {
        return Objects.hash(sDate, sTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof StockHistoryDateTime)) {
            return false;
        }

        var other = (StockHistoryDateTime) obj;
        return Objects.equals(sDate, other.sDate) && Objects.equals(sTime, other.sTime);
    }

    @Override
    public String toString() {
        return "(s_date=" + sDate + ", s_time=" + sTime + ")";
    }
}
