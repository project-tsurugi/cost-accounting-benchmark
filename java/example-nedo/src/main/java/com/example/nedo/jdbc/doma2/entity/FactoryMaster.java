package com.example.nedo.jdbc.doma2.entity;

import org.seasar.doma.Column;
import org.seasar.doma.Entity;
import org.seasar.doma.Id;
import org.seasar.doma.Table;

@Entity
@Table(name = "factory_master")
public class FactoryMaster implements Cloneable {

    /** unique ID(4) */
    @Column(name = "f_id")
    @Id
    Integer fId;

    /** variable text(20) */
    @Column(name = "f_name")
    String fName;

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
}
