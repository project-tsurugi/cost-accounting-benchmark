package com.example.nedo.jdbc.doma2.config;

import javax.sql.DataSource;

import org.seasar.doma.SingletonConfig;
import org.seasar.doma.jdbc.Config;
import org.seasar.doma.jdbc.JdbcLogger;
import org.seasar.doma.jdbc.dialect.Dialect;
import org.seasar.doma.jdbc.tx.LocalTransactionDataSource;
import org.seasar.doma.jdbc.tx.LocalTransactionManager;
import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.BenchConst;
import com.example.nedo.jdbc.doma2.log.MyLoggingJdbcLogger;

// https://doma.readthedocs.io/en/2.5.1/transaction/
@SingletonConfig
public class AppConfig implements Config {

	private static final AppConfig CONFIG = new AppConfig();

	private final Dialect dialect;
	private final LocalTransactionDataSource dataSource;
	private final TransactionManager transactionManager;

	private AppConfig() {
		JdbcConfigFactory configFactory = createConfigFactory(BenchConst.jdbcUrl());

		this.dialect = configFactory.createDialect();
		this.dataSource = new LocalTransactionDataSource(configFactory.createDataSource());
		this.transactionManager = new LocalTransactionManager(dataSource.getLocalTransaction(getJdbcLogger()));
	}

	private JdbcConfigFactory createConfigFactory(String url) {
		if (url.contains("oracle")) {
			return new OracleConfigFactory();
		} else if (url.contains("postgresql")) {
			return new PostgresConfigFactory();
		} else {
			throw new UnsupportedOperationException("unsupported jdbc url: " + url);
		}
	}

	@Override
	public Dialect getDialect() {
		return dialect;
	}

	@Override
	public DataSource getDataSource() {
		return dataSource;
	}

	@Override
	public TransactionManager getTransactionManager() {
		return transactionManager;
	}

	@Override
	public JdbcLogger getJdbcLogger() {
		return new MyLoggingJdbcLogger();
	}

	public static AppConfig singleton() {
		return CONFIG;
	}
}
