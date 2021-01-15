package com.example.nedo.init;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.jdbc.doma2.config.AppConfig;

@SuppressWarnings("serial")
public abstract class DaoListTask<T> extends RecursiveTask<Void> {
	private final List<T> list = new ArrayList<>();

	public void add(T t) {
		list.add(t);
	}

	public int size() {
		return list.size();
	}

	@Override
	protected final Void compute() {
		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		tm.required(() -> {
			for (T t : list) {
				execute(t);
			}
		});
		return null;
	}

	protected abstract void execute(T t);
}
