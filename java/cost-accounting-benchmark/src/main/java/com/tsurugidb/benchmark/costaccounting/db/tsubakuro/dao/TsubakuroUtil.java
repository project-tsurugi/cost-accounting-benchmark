package com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalTime;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.ResultSet;

public class TsubakuroUtil {

    public static Parameter getParameter(String name, Integer value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter getParameter(String name, String value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter getParameter(String name, BigInteger value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, new BigDecimal(value));
    }

    public static Parameter getParameter(String name, BigDecimal value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter getParameter(String name, BigDecimal value, int scale) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return getParameter(name, value.setScale(scale, RoundingMode.HALF_UP));
    }

    public static Parameter getParameter(String name, LocalDate value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter getParameter(String name, LocalTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter getParameter(String name, MeasurementType value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value.getValue());
    }

    public static Parameter getParameter(String name, ItemType value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value.getValue());
    }

    //

    public static Integer getInt(ResultSet rs, int index) throws IOException, ServerException, InterruptedException {
        return getValue(rs, index, type -> {
            switch (type) {
            case INT4:
                return rs.fetchInt4Value();
            default:
                throw new UnsupportedOperationException("atomType=" + type);
            }
        });
    }

    public static BigInteger getBigInt(ResultSet rs, int index) throws IOException, ServerException, InterruptedException {
        BigDecimal d = getDecimal(rs, index);
        return (d != null) ? d.toBigInteger() : null;
    }

    public static BigDecimal getDecimal(ResultSet rs, int index) throws IOException, ServerException, InterruptedException {
        return getValue(rs, index, type -> {
            switch (type) {
            case DECIMAL:
                return rs.fetchDecimalValue();
            default:
                throw new UnsupportedOperationException("atomType=" + type);
            }
        });
    }

    public static LocalDate getDate(ResultSet rs, int index) throws IOException, ServerException, InterruptedException {
        return getValue(rs, index, type -> {
            switch (type) {
            case DATE:
                return rs.fetchDateValue();
            default:
                throw new UnsupportedOperationException("atomType=" + type);
            }
        });
    }

    public static LocalTime getTime(ResultSet rs, int index) throws IOException, ServerException, InterruptedException {
        return getValue(rs, index, type -> {
            switch (type) {
            case TIME_OF_DAY:
                return rs.fetchTimeOfDayValue();
            default:
                throw new UnsupportedOperationException("atomType=" + type);
            }
        });
    }

    public static String getString(ResultSet rs, int index) throws IOException, ServerException, InterruptedException {
        return getValue(rs, index, type -> {
            switch (type) {
            case CHARACTER:
                return rs.fetchCharacterValue();
            default:
                throw new UnsupportedOperationException("atomType=" + type);
            }
        });
    }

    @FunctionalInterface
    private interface ValueFunction<V> {
        V apply(AtomType type) throws IOException, ServerException, InterruptedException;
    }

    private static <V> V getValue(ResultSet rs, int index, ValueFunction<V> converter) throws IOException, ServerException, InterruptedException {
        if (rs.isNull()) {
            return null;
        }
        var column = rs.getMetadata().getColumns().get(index);
        return converter.apply(column.getAtomType());
    }

    public static MeasurementType getMeasurementType(ResultSet rs, int index) throws IOException, ServerException, InterruptedException {
        String s = getString(rs, index);
        return (s != null) ? MeasurementType.of(s) : null;
    }

    public static ItemType getItemType(ResultSet rs, int index) throws IOException, ServerException, InterruptedException {
        String s = getString(rs, index);
        return (s != null) ? ItemType.of(s) : null;
    }
}
