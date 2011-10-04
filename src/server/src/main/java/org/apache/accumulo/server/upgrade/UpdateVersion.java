package org.apache.accumulo.server.upgrade;

import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class UpdateVersion {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = CachedConfiguration.getInstance();
		FileSystem fs = FileSystem.get(conf);
		
		Path location = ServerConstants.getDataVersionLocation();
		
		fs.delete(new Path(location, "2"), true);
		fs.mkdirs(new Path(location, "3"));
		
	}
}
