package org.apache.accumulo.server.monitor.util.celltypes;

import java.net.InetSocketAddress;

import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.util.AddressUtil;


public class TServerLinkType extends CellType<TabletServerStatus> {

	@Override
	public String format(Object obj) {
		if (obj == null)
			return "-";
		TabletServerStatus status = (TabletServerStatus) obj;
		return String.format("<a href='/tservers?s=%s'>%s</a>", status.name, displayName(status));
	}

	public static String displayName(TabletServerStatus status) {
		if (status.name == null)
			return "--Unknown--";
		InetSocketAddress address = AddressUtil.parseAddress(status.name, 0);
		return address.getHostName() + ":" + address.getPort();
	}

	@Override
	public int compare(TabletServerStatus o1, TabletServerStatus o2) {
		return displayName(o1).compareTo(displayName(o2));
	}

	@Override
	public String alignment() {
		return "left";
	}

}
