package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

public class IceaxeRecordUtil {

    // get from ResultRecord

    public static Integer getInt(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        return record.nextIntOrNull();
    }

    public static BigInteger getBigInt(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        BigDecimal value = getDecimal(record);
        return (value == null) ? null : value.toBigInteger();
    }

    public static BigDecimal getDecimal(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        return record.nextDecimalOrNull();
    }

    public static LocalDate getDate(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        return record.nextDateOrNull();
    }

    public static String getString(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        return record.nextStringOrNull();
    }

    public static MeasurementType getMeasurementType(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        String value = getString(record);
        return (value == null) ? null : MeasurementType.of(value);
    }

    public static ItemType getItemType(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        String value = getString(record);
        return (value == null) ? null : ItemType.of(value);
    }
}
