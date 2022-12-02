package com.tsurugidb.benchmark.costaccounting.generate.entity;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.poi.ss.usermodel.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;
import com.tsurugidb.benchmark.costaccounting.generate.util.WriterWrapper;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class TableEntityWriter extends WriterWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(TableEntityWriter.class);

    private final TableSheet table;

    private final Set<String> importSet = new TreeSet<>();

    public TableEntityWriter(TableSheet table, BufferedWriter writer) {
        super(writer, "    ");
        this.table = table;
    }

    public void convert() throws IOException {
        LOG.info("sheet={}", table.getSheetName());

        writePackage();
        writeImport();
        writeClassName();
        writeStaticField();
        writeField();
        writeSetterGetter();
        writeClone();
        writeDateRange();
        writeToCsv();
        writeToString();
        writeln("}");
    }

    private void writePackage() throws IOException {
        writeln("package ", BenchConst.PACKAGE_ENTITY, ";");
        writeln();
    }

    private void writeImport() throws IOException {
        table.getRows().forEach(row -> {
            // initialize importSet
            getType(row);
        });

        if (!importSet.isEmpty()) {
            for (String s : importSet) {
                writeln("import ", s, ";");
            }
            writeln();
        }
    }

    private void writeClassName() throws IOException {
        writeln("/**");
        writeln(" * ", table.getTableName());
        writeln(" */");
        String dateRange = table.hasDateRange() ? ", HasDateRange" : "";
        writeln("public class ", table.getClassName(), " implements Cloneable", dateRange, " {");
    }

    private void writeStaticField() {
        table.getRows().filter(row -> table.getColumnType(row).contains("numeric")).forEachOrdered(row -> {
            Integer scale = table.getColumnTypeScale(row);
            if (scale != null) {
                try {
                    writeln();
                    writeln(1, "/** ", table.getColumnName(row), " ", getTypeComment(row), " */");
                    writeln(1, "public static final int ", table.getColumnName(row).toUpperCase() + "_SCALE", " = ", scale.toString(), ";");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    private void writeField() {
        table.getRows().forEachOrdered(row -> {
            try {
                writeln();
                writeln(1, "/** ", table.getColumnName(row), " ", getTypeComment(row), " */");
                writeln(1, "private ", getType(row), " ", table.getColumnFieldName(row), ";");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    protected String getType(Row row) {
        String columnName = table.getColumnName(row);
        switch (columnName) {
        case "i_type":
            importSet.add(BenchConst.PACKAGE_DOMAIN + ".ItemType");
            return "ItemType";
        case "m_type":
            importSet.add(BenchConst.PACKAGE_DOMAIN + ".MeasurementType");
            return "MeasurementType";
        default:
            break;
        }

        String typeName = table.getColumnType(row);
        if (typeName == null) {
            return "";
        }
        switch (typeName) {
        case "unique ID":
            return getTypeUniqueId(row);
        case "unsigned numeric":
            return getTypeNumeric(row);
        case "variable text":
            return "String";
        case "date":
            importSet.add("java.time.LocalDate");
            return "LocalDate";
        default:
            throw new UnsupportedOperationException(typeName);
        }
    }

    protected String getTypeUniqueId(Row row) {
        Integer size = table.getColumnTypeSize(row);
        if (size != null) {
            int s = size;
            if (s <= 9) {
                return "Integer";
            }
            if (s <= 18) {
                return "Long";
            }
        }

        return getTypeNumeric(row);
    }

    protected String getTypeNumeric(Row row) {
        Integer scale = table.getColumnTypeScale(row);
        if (scale == null || scale.intValue() == 0) {
            importSet.add("java.math.BigInteger");
            return "BigInteger";
        }

        importSet.add("java.math.BigDecimal");
        return "BigDecimal";
    }

    protected String getTypeComment(Row row) {
        String type = table.getColumnType(row);
        Integer size = table.getColumnTypeSize(row);
        Integer scale = table.getColumnTypeScale(row);
        if (size == null) {
            return type;
        }
        if (scale == null) {
            return type + "(" + size + ")";
        } else {
            return type + "(" + size + ", " + scale + ")";
        }
    }

    protected boolean isPrimaryKey(Row row) {
        return table.getColumnKey(row) != null;
    }

    protected void writeSetterGetter() {
        table.getRows().forEachOrdered(row -> {
            try {
                writeSetter(row);
                writeGetter(row);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private Set<String> methodNameSet = new HashSet<>();

    protected void writeSetter(Row row) throws IOException {
        String methodName = "set" + table.getColumnMethodName(row);
        methodNameSet.add(methodName);

        writeln();
        writeln(1, "public void ", methodName, "(", getType(row), " value) {");
        writeln(2, "this.", table.getColumnFieldName(row), " = value;");
        writeln(1, "}");
    }

    protected void writeGetter(Row row) throws IOException {
        String methodName = "get" + table.getColumnMethodName(row);
        methodNameSet.add(methodName);

        writeln();
        writeln(1, "public ", getType(row), " ", methodName, "() {");
        writeln(2, "return this.", table.getColumnFieldName(row), ";");
        writeln(1, "}");
    }

    protected void writeClone() throws IOException {
        String className = table.getClassName();
        writeln();
        writeln(1, "@Override");
        writeln(1, "public ", className, " clone() {");
        writeln(2, "try {");
        writeln(3, "return (", className, ") super.clone();");
        writeln(2, "} catch (CloneNotSupportedException e) {");
        writeln(3, "throw new InternalError(e);");
        writeln(2, "}");
        writeln(1, "}");
    }

    protected void writeDateRange() throws IOException {
        if (!table.hasDateRange()) {
            return;
        }

        writeln();
        writeln(1, "@Override");
        writeln(1, "public LocalDate getEffectiveDate() {");
        writeln(2, "return ", findMethodName("get", "EffectiveDate"), "();");
        writeln(1, "}");

        writeln();
        writeln(1, "@Override");
        writeln(1, "public void setEffectiveDate(LocalDate value) {");
        writeln(2, findMethodName("set", "EffectiveDate"), "(value);");
        writeln(1, "}");

        writeln();
        writeln(1, "@Override");
        writeln(1, "public LocalDate getExpiredDate() {");
        writeln(2, "return ", findMethodName("get", "ExpiredDate"), "();");
        writeln(1, "}");

        writeln();
        writeln(1, "@Override");
        writeln(1, "public void setExpiredDate(LocalDate value) {");
        writeln(2, findMethodName("set", "ExpiredDate"), "(value);");
        writeln(1, "}");
    }

    private String findMethodName(String prefix, String suffix) {
        for (String s : methodNameSet) {
            if (s.startsWith(prefix) && s.endsWith(suffix)) {
                return s;
            }
        }
        return null;
    }

    protected void writeToCsv() throws IOException {
        writeln();
        writeln(1, "public String toCsv(String suffix) {");

        String s = table.getRows().map(row -> {
            String fname = table.getColumnFieldName(row);
            return fname;
        }).collect(Collectors.joining(" + \",\" + "));
        writeln(2, "return " + s + " + suffix;");

        writeln(1, "}");
    }

    protected void writeToString() throws IOException {
        writeln();
        writeln(1, "@Override");
        writeln(1, "public String toString() {");

        String tableName = table.getClassName();
        String s = table.getRows().map(row -> {
            String cname = table.getColumnName(row);
            String fname = table.getColumnFieldName(row);
            return cname + "=\" + " + fname;
        }).collect(Collectors.joining(" + \", ", "\"" + tableName + "(", " + \")\""));
        writeln(2, "return " + s + ";");

        writeln(1, "}");
    }
}
