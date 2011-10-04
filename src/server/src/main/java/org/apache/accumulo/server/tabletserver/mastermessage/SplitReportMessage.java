package org.apache.accumulo.server.tabletserver.mastermessage;

import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.impl.Translator;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.TabletSplit;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;


public class SplitReportMessage implements MasterMessage {
	Map<KeyExtent, Text> extents;
	KeyExtent old_extent;
	
	public SplitReportMessage(KeyExtent old_extent, Map<KeyExtent, Text> newExtents)
	{
		this.old_extent = old_extent;
		extents = new TreeMap<KeyExtent, Text>(newExtents);
	}

	public SplitReportMessage(KeyExtent old_extent, KeyExtent ne1, Text np1, KeyExtent ne2, Text np2) {
		this.old_extent = old_extent;
		extents = new TreeMap<KeyExtent, Text>();
		extents.put(ne1, np1);
		extents.put(ne2, np2);
	}

	public void send(AuthInfo credentials, String serverName, MasterClientService.Iface client) throws TException, ThriftSecurityException {
		TabletSplit split = new TabletSplit();
		split.oldTablet = old_extent.toThrift();
		split.newTablets = Translator.translate(extents.keySet(), Translator.KET);
		client.reportSplitExtent(null, credentials, serverName, split);		
	}

}
