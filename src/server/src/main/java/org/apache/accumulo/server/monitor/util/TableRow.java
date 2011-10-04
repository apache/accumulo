package org.apache.accumulo.server.monitor.util;

import java.util.ArrayList;
import java.util.Comparator;

public class TableRow {
	private int size;
	private ArrayList<Object> row;

	TableRow(int size) {
		this.size = size;
		this.row = new ArrayList<Object>(size);
	}

	public boolean add(Object obj) {
		if (row.size() == size)
			throw new IllegalStateException("Row is full.");
		return row.add(obj);
	}

	Object get(int index) {
		return row.get(index);
	}

	int size() {
		return row.size();
	}

	Object set(int i, Object value) {
		return row.set(i, value);
	}

	public static <T> Comparator<TableRow> getComparator(int index, Comparator<T> comp) {
		return new TableRowComparator<T>(index, comp);
	}

	private static class TableRowComparator<T> implements Comparator<TableRow> {
		private int index;
		private Comparator<T> comp;

		public TableRowComparator(int index, Comparator<T> comp) {
			this.index = index;
			this.comp = comp;
		}

		@SuppressWarnings("unchecked")
		@Override
		public int compare(TableRow o1, TableRow o2) {
			return comp.compare((T) o1.get(index), (T) o2.get(index));
		}
	}
}
