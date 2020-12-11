package com.example.nedo.jdbc.doma2.config;

import javax.sql.DataSource;

import org.seasar.doma.SingletonConfig;
import org.seasar.doma.jdbc.Config;
import org.seasar.doma.jdbc.JdbcLogger;
import org.seasar.doma.jdbc.dialect.Dialect;
import org.seasar.doma.jdbc.dialect.OracleDialect;
import org.seasar.doma.jdbc.dialect.PostgresDialect;
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
		String url = BenchConst.jdbcUrl();
		String user = BenchConst.jdbcUser();
		String password = BenchConst.jdbcPassword();

		this.dialect = createDialect(url);
		this.dataSource = new LocalTransactionDataSource(url, user, password);
		this.transactionManager = new LocalTransactionManager(dataSource.getLocalTransaction(getJdbcLogger()));
	}

	private Dialect createDialect(String url) {
		if (url.contains("oracle")) {
			return new OracleDialect();
		} else {
			return new PostgresDialect();
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
