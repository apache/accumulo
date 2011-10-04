package org.apache.accumulo.server.monitor.util;

import org.apache.accumulo.server.monitor.util.celltypes.CellType;
import org.apache.accumulo.server.monitor.util.celltypes.StringType;

public class TableColumn<T> {
	private String title;
	private CellType<T> type;
	private String legend;

	public TableColumn(String title, CellType<T> type, String legend) {
		this.title = title;
		this.type = type != null ? type : new StringType<T>();
		this.legend = legend;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getTitle() {
		return title;
	}

	public String getLegend() {
		return legend;
	}

	public CellType<T> getCellType() {
		return type;
	}
}
