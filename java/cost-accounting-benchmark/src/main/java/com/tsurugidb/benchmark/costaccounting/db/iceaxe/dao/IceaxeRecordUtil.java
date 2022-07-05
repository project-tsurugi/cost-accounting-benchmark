package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;
import com.tsurugidb.iceaxe.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

//TODO dataType
public class IceaxeRecordUtil {

    // get from ResultRecord

    public static Integer getInt(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        return record.nextInt4OrNull();
    }

    public static BigInteger getBigInt(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        BigDecimal value = getDecimal(record);
        return (value == null) ? null : value.toBigInteger();
    }

    public static BigDecimal getDecimal(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
//      return record.nextDecimalOrNull();
        Double value = record.nextFloat8OrNull();
        return (value == null) ? null : BigDecimal.valueOf(value);
    }

    public static LocalDate getDate(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
//      return record.nextDateOrNull();
        String value = record.nextCharacterOrNull();
        return (value == null) ? null : LocalDate.parse(value);
    }

    public static String getString(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
        return record.nextCharacterOrNull();
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
