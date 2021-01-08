package com.example.nedo.batch;

import java.util.List;

public class StringUtil {

	public static String toString(List<Integer> list) {
		StringBuilder sb = new StringBuilder();
		sb.append('[');

		int end = 0;
		int count = 0;
		String comma = "";
		for (int i = 0; i < list.size() + 1; i++) {
			int n = Integer.MAX_VALUE;
			if (i < list.size()) {
				n = list.get(i);
				if (i > 0 && n == list.get(i - 1).intValue() + 1) {
					end = n;
					count++;
					continue;
				}
			}

			if (end > 0) {
				if (count > 2) {
					sb.append('-');
				} else {
					sb.append(", ");
				}
				sb.append(end);
				end = 0;
			}

			if (n != Integer.MAX_VALUE) {
				sb.append(comma);
				comma = ", ";
				sb.append(n);
				count = 1;
			}
		}

		sb.append(']');
		return sb.toString();
	}
}
