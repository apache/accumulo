package org.apache.accumulo.server.monitor.util.celltypes;

import java.util.Comparator;

public abstract class CellType<T> implements Comparator<T> {
	private boolean sortable = true;

	abstract public String alignment();

	abstract public String format(Object obj);

	public final void setSortable(boolean sortable) {
		this.sortable = sortable;
	}

	public final boolean isSortable() {
		return sortable;
	}
}
