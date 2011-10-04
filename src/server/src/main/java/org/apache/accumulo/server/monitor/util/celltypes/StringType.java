package org.apache.accumulo.server.monitor.util.celltypes;

public class StringType<T> extends CellType<T> {
	@Override
	public String format(Object obj) {
		return obj == null ? "-" : obj.toString();
	}

	@Override
	public int compare(T o1, T o2) {
		if (o1 == null && o2 == null)
			return 0;
		else if (o1 == null)
			return -1;
		else if (o2 == null)
			return 1;
		return o1.toString().compareTo(o2.toString());
	}

	@Override
	public String alignment() {
		return "left";
	}
}
