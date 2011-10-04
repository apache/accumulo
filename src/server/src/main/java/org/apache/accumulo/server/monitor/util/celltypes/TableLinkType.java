package org.apache.accumulo.server.monitor.util.celltypes;

import java.util.Map;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.server.client.HdfsZooInstance;


public class TableLinkType extends CellType<String> {

	private Map<String, String> tidToNameMap;

	public TableLinkType() {
		tidToNameMap = Tables.getIdToNameMap(HdfsZooInstance.getInstance());
	}

	@Override
	public String format(Object obj) {
		if (obj == null)
			return "-";
		String tableId = (String) obj;
		return String.format("<a href='/tables?t=%s'>%s</a>", tableId, displayName(tableId));
	}

	private String displayName(String tableId) {
		if (tableId == null)
			return "-";
		return Tables.getPrintableTableNameFromId(tidToNameMap, tableId);
	}

	@Override
	public int compare(String o1, String o2) {
		return displayName(o1).compareTo(displayName(o2));
	}

	@Override
	public String alignment() {
		return "left";
	}

}
