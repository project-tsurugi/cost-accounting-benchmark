package com.example.nedo.init.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveAction;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.jdbc.doma2.config.AppConfig;

@SuppressWarnings("serial")
public abstract class DaoListTask<T> extends RecursiveAction {
	private final List<T> list = new ArrayList<>();

	public void add(T t) {
		list.add(t);
	}

	public int size() {
		return list.size();
	}

	@Override
	protected final void compute() {
		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		tm.required(() -> {
			for (T t : list) {
				execute(t);
			}
		});
	}

	protected abstract void execute(T t);
}
