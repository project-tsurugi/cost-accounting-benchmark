/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

public class IceaxeRecordUtil {

    // get from ResultRecord

    public static Integer getInt(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        return record.nextIntOrNull();
    }

    public static BigInteger getBigInt(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        BigDecimal value = getDecimal(record);
        return (value == null) ? null : value.toBigInteger();
    }

    public static BigDecimal getDecimal(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        return record.nextDecimalOrNull();
    }

    public static LocalDate getDate(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        return record.nextDateOrNull();
    }

    public static LocalTime getTime(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        return record.nextTimeOrNull();
    }

    public static String getString(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        return record.nextStringOrNull();
    }

    public static MeasurementType getMeasurementType(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        String value = getString(record);
        return (value == null) ? null : MeasurementType.of(value);
    }

    public static ItemType getItemType(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        String value = getString(record);
        return (value == null) ? null : ItemType.of(value);
    }
}
