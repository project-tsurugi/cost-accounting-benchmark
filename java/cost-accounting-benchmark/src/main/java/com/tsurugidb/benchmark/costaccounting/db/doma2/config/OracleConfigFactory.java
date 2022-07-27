package com.tsurugidb.benchmark.costaccounting.db.doma2.config;

import java.sql.SQLException;
import javax.sql.DataSource;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import org.seasar.doma.jdbc.dialect.Dialect;
import org.seasar.doma.jdbc.dialect.OracleDialect;

import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class OracleConfigFactory implements JdbcConfigFactory {

    @Override
    public DataSource createDataSource() {
        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
        try {
            pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
            pds.setURL(BenchConst.jdbcUrl());
            pds.setUser(BenchConst.jdbcUser());
            pds.setPassword(BenchConst.jdbcPassword());
            pds.setMaxStatements(256);
            return pds;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Dialect createDialect() {
        return new OracleDialect();
    }

}
