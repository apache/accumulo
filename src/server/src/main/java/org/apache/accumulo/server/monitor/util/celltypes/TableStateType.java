package org.apache.accumulo.server.monitor.util.celltypes;

import org.apache.accumulo.server.master.state.tables.TableState;

public class TableStateType extends CellType<TableState> {

	@Override
	public String alignment() {
		return "center";
	}

	@Override
	public String format(Object obj) {
		TableState state = obj == null ? TableState.UNKNOWN : (TableState) obj;
		String style = null;
		switch (state) {
		case ONLINE:
		case DISABLED:
			break;
		case NEW:
		case DELETING:
		case DISABLING:
		case UNLOADING:
		case LOADING:
			style = "warning";
			break;
		case OFFLINE:
		case UNKNOWN:
		default:
			style = "error";
		}
		style = style != null ? " class='" + style + "'" : "";
		return String.format("<span%s>%s</span>", style, state);
	}

	@Override
	public int compare(TableState o1, TableState o2) {
		if (o1 == null && o2 == null)
			return 0;
		else if (o1 == null)
			return -1;
		return o1.compareTo(o2);
	}

}
