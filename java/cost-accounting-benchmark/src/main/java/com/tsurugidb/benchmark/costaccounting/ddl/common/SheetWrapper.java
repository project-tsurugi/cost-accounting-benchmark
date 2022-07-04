package com.tsurugidb.benchmark.costaccounting.ddl.common;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellReference;

public abstract class SheetWrapper {

    protected final Sheet sheet;

    public SheetWrapper(Sheet sheet) {
        this.sheet = sheet;
    }

    public String getSheetName() {
        return sheet.getSheetName();
    }

    public Stream<Row> getRows() {
        Iterator<Row> i = new Iterator<Row>() {

            private int rowIndex = getStartRowIndex();

            @Override
            public boolean hasNext() {
                Row row = sheet.getRow(rowIndex);
                return hasData(row);
            }

            @Override
            public Row next() {
                Row row = sheet.getRow(rowIndex);
                if (row != null) {
                    rowIndex++;
                }
                return row;
            }
        };

        Spliterator<Row> spliterator = Spliterators.spliteratorUnknownSize(i, Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false);
    }

    protected abstract int getStartRowIndex();

    public String toCamelCase(String snakeCase, boolean firstUpper) {
        StringBuilder sb = new StringBuilder(snakeCase.length());

        boolean[] first = { firstUpper };
        snakeCase.codePoints().forEachOrdered(c -> {
            if (first[0]) {
                sb.appendCodePoint(Character.toUpperCase(c));
                first[0] = false;
            } else {
                if (c == '_') {
                    first[0] = true;
                } else {
                    sb.appendCodePoint(Character.toLowerCase(c));
                }
            }
        });

        return sb.toString();
    }

    public boolean hasNext(Row row) {
        int nextIndex = row.getRowNum() + 1;
        Row nextRow = row.getSheet().getRow(nextIndex);
        return hasData(nextRow);
    }

    public abstract boolean hasData(Row row);

    // Sheet操作

    public String getCellAsString(Sheet sheet, String position) {
        CellReference ref = new CellReference(position);
        return getCellAsString(sheet, ref.getRow(), ref.getCol());
    }

    public String getCellAsString(Sheet sheet, int rowIndex, int colIndex) {
        Row row = sheet.getRow(rowIndex);
        if (row == null) {
            return null;
        }
        return getCellAsString(row, colIndex);
    }

    public String getCellAsString(Row row, int colIndex) {
        Cell cell = row.getCell(colIndex);
        if (cell == null) {
            return null;
        }
        switch (cell.getCellType()) {
        case STRING:
        case FORMULA:
            return cell.getStringCellValue();
        case BLANK:
            return null;
        default:
            throw new UnsupportedOperationException(cell.getCellType().name());
        }
    }

    public Integer getCellAsInteger(Row row, int colIndex) {
        Cell cell = row.getCell(colIndex);
        if (cell == null) {
            return null;
        }
        switch (cell.getCellType()) {
        case NUMERIC:
        case FORMULA:
            return (int) cell.getNumericCellValue();
        case BLANK:
            return null;
        default:
            throw new UnsupportedOperationException(cell.getCellType().name());
        }
    }

    public BigDecimal getCellAsDecimal(Row row, int colIndex) {
        Cell cell = row.getCell(colIndex);
        if (cell == null) {
            return null;
        }
        switch (cell.getCellType()) {
        case NUMERIC:
        case FORMULA:
            return BigDecimal.valueOf(cell.getNumericCellValue());
        case BLANK:
            return null;
        default:
            throw new UnsupportedOperationException(cell.getCellType().name());
        }
    }
}
