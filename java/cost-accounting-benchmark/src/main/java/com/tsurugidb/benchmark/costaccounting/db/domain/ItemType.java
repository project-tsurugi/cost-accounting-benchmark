package com.tsurugidb.benchmark.costaccounting.db.domain;

public enum ItemType {
    /** 製品 */
    PRODUCT,
    /** 中間材 */
    WORK_IN_PROCESS,
    /** 原料 */
    RAW_MATERIAL;

    public static ItemType of(String value) {
        return ItemType.valueOf(value.toUpperCase());
    }

    public String getValue() {
        return name().toLowerCase();
    }
}
