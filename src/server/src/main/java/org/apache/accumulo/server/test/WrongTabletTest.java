package org.apache.accumulo.server.test;

import java.nio.ByteBuffer;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.hadoop.io.Text;


public class WrongTabletTest {
	private static AuthInfo rootCredentials = new AuthInfo("root", ByteBuffer.wrap("secret".getBytes()), HdfsZooInstance.getInstance().getInstanceID());
	
	public static void main(String[] args) {
	    String location = args[0];
		try {
			TabletClientService.Iface client = ThriftUtil.getTServerClient(location,ServerConfiguration.getSystemConfiguration());
			
			Mutation mutation = new Mutation(new Text("row_0003750001"));
			//mutation.set(new Text("colf:colq"), new Value("val".getBytes()));
			mutation.putDelete(new Text("colf"), new Text("colq"));
			client.update(null, rootCredentials, new KeyExtent(new Text("test_ingest"), null, new Text("row_0003750000")).toThrift(), mutation.toThrift());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
