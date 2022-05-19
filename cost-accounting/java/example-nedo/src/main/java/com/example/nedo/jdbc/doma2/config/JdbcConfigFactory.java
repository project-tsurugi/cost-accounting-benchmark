package com.example.nedo.jdbc.doma2.config;

import javax.sql.DataSource;

import org.seasar.doma.jdbc.dialect.Dialect;

interface JdbcConfigFactory {

    DataSource createDataSource();

    Dialect createDialect();

}
