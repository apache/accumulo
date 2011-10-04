package org.apache.accumulo.server.monitor.util.celltypes;

import java.net.InetSocketAddress;

import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.util.AddressUtil;


public class LoggerLinkType extends CellType<RecoveryStatus> {

	@Override
	public String format(Object obj) {
		if (obj == null)
			return "-";
		RecoveryStatus status = (RecoveryStatus) obj;
		return String.format("<a href='/loggers?s=%s'>%s</a>", status.host, displayName(status));
	}

	public static String displayName(RecoveryStatus status) {
		if (status.host == null)
			return "--Unknown--";
		InetSocketAddress monitorAddress = AddressUtil.parseAddress(status.host, 0);
		return monitorAddress.getHostName() + ":" + monitorAddress.getPort();
	}

	@Override
	public int compare(RecoveryStatus o1, RecoveryStatus o2) {
		return displayName(o1).compareTo(displayName(o2));
	}

	@Override
	public String alignment() {
		return "left";
	}

}
