package com.tsurugidb.benchmark.costaccounting.db.doma2.config;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;

import org.seasar.doma.jdbc.dialect.Dialect;
import org.seasar.doma.jdbc.dialect.PostgresDialect;

import com.tsurugidb.benchmark.costaccounting.BenchConst;

public class PostgresConfigFactory implements JdbcConfigFactory {

    @Override
    public DataSource createDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setURL(BenchConst.jdbcUrl());
        ds.setUser(BenchConst.jdbcUser());
        ds.setPassword(BenchConst.jdbcPassword());
        return ds;
    }

    @Override
    public Dialect createDialect() {
        return new PostgresDialect();
    }

}
