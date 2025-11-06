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
package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalTime;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;

public class JdbcUtil {

    // set to PreparedStatement

    public static void setInt(PreparedStatement ps, int i, Integer value) throws SQLException {
        if (value != null) {
            ps.setInt(i, value);
        } else {
            ps.setNull(i, Types.INTEGER);
        }
    }

    public static void setBigInt(PreparedStatement ps, int i, BigInteger value) throws SQLException {
        if (value != null) {
            ps.setBigDecimal(i, new BigDecimal(value));
        } else {
            ps.setBigDecimal(i, null);
        }
    }

    public static void setDecimal(PreparedStatement ps, int i, BigDecimal value) throws SQLException {
        ps.setBigDecimal(i, value);
    }

    public static void setDate(PreparedStatement ps, int i, LocalDate value) throws SQLException {
        ps.setDate(i, sdate(value));
    }

    public static void setTime(PreparedStatement ps, int i, LocalTime value) throws SQLException {
        ps.setTime(i, stime(value));
    }

    public static void setString(PreparedStatement ps, int i, String value) throws SQLException {
        ps.setString(i, value);
    }

    public static void setMeasurementType(PreparedStatement ps, int i, MeasurementType value) throws SQLException {
        if (value != null) {
            ps.setString(i, value.getValue());
        } else {
            ps.setString(i, null);
        }
    }

    public static void setItemType(PreparedStatement ps, int i, ItemType value) throws SQLException {
        if (value != null) {
            ps.setString(i, value.getValue());
        } else {
            ps.setString(i, null);
        }
    }

    // get from ResultSet

    public static Integer getInt(ResultSet rs, String name) throws SQLException {
        int value = rs.getInt(name);
        return rs.wasNull() ? null : value;
    }

    public static BigInteger getBigInt(ResultSet rs, String name) throws SQLException {
        BigDecimal value = rs.getBigDecimal(name);
        return (value == null) ? null : value.toBigInteger();
    }

    public static BigDecimal getDecimal(ResultSet rs, String name) throws SQLException {
        return rs.getBigDecimal(name);
    }

    public static LocalDate getDate(ResultSet rs, String name) throws SQLException {
        return ldate(rs.getDate(name));
    }

    public static LocalTime getTime(ResultSet rs, String name) throws SQLException {
        return ltime(rs.getTime(name));
    }

    public static String getString(ResultSet rs, String name) throws SQLException {
        return rs.getString(name);
    }

    public static MeasurementType getMeasurementType(ResultSet rs, String name) throws SQLException {
        String value = rs.getString(name);
        return (value == null) ? null : MeasurementType.of(value);
    }

    public static ItemType getItemType(ResultSet rs, String name) throws SQLException {
        String value = rs.getString(name);
        return (value == null) ? null : ItemType.of(value);
    }

    //

    private static java.sql.Date sdate(LocalDate date) {
        if (date == null) {
            return null;
        }
        return java.sql.Date.valueOf(date);
    }

    private static LocalDate ldate(java.sql.Date date) {
        if (date == null) {
            return null;
        }
        return date.toLocalDate();
    }

    private static java.sql.Time stime(LocalTime time) {
        if (time == null) {
            return null;
        }
        return java.sql.Time.valueOf(time);
    }

    private static LocalTime ltime(java.sql.Time time) {
        if (time == null) {
            return null;
        }
        return time.toLocalTime();
    }
}
