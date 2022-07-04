package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;

import com.tsurugidb.benchmark.costaccounting.db.doma2.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.doma2.domain.MeasurementType;
import com.tsurugidb.iceaxe.result.TsurugiResultRecord;

public class IceaxeRecordUtil {

    // get from ResultRecord

    public static Integer getInt(TsurugiResultRecord record) throws IOException {
        return record.nextInt4OrNull();
    }

    public static BigInteger getBigInt(TsurugiResultRecord record) throws IOException {
        BigDecimal value = getDecimal(record);
        return (value == null) ? null : value.toBigInteger();
    }

    public static BigDecimal getDecimal(TsurugiResultRecord record) throws IOException {
        return record.nextDecimalOrNull();
    }

    public static LocalDate getDate(TsurugiResultRecord record) throws IOException {
        return record.nextDateOrNull();
    }

    public static String getString(TsurugiResultRecord record) throws IOException {
        return record.nextCharacterOrNull();
    }

    public static MeasurementType getMeasurementType(TsurugiResultRecord record) throws IOException {
        String value = getString(record);
        return (value == null) ? null : MeasurementType.of(value);
    }

    public static ItemType getItemType(TsurugiResultRecord record) throws IOException {
        String value = getString(record);
        return (value == null) ? null : ItemType.of(value);
    }
}
