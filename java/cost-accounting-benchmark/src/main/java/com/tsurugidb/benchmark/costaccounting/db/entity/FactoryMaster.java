package com.tsurugidb.benchmark.costaccounting.db.entity;

/**
 * factory_master
 */
public class FactoryMaster implements Cloneable {

    /** f_id unique ID(4) */
    private Integer fId;

    /** f_name variable text(20) */
    private String fName;

    public void setFId(Integer value) {
        this.fId = value;
    }

    public Integer getFId() {
        return this.fId;
    }

    public void setFName(String value) {
        this.fName = value;
    }

    public String getFName() {
        return this.fName;
    }

    @Override
    public FactoryMaster clone() {
        try {
            return (FactoryMaster) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    public String toCsv(String suffix) {
        return fId + "," + fName + suffix;
    }

    @Override
    public String toString() {
        return "FactoryMaster(f_id=" + fId + ", f_name=" + fName + ")";
    }
}
