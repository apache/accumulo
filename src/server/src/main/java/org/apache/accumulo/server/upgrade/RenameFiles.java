package org.apache.accumulo.server.upgrade;

import java.util.SortedMap;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class RenameFiles {
	public static void main(String[] args) throws Exception {
		String tablesDir = ServerConstants.getTablesDir();
		System.out.println(" "+tablesDir);
		SortedMap<String, String> tableIds = Tables.getNameToIdMap(HdfsZooInstance.getInstance());
		
		Configuration conf = CachedConfiguration.getInstance();
		FileSystem fs = FileSystem.get(conf);
		
		FileStatus[] tables = fs.listStatus(new Path(tablesDir));

		for (FileStatus tableStatus : tables) {
			String tid = tableIds.get(tableStatus.getPath().getName());
			Path newPath = new Path(tableStatus.getPath().getParent(), tid);
			fs.rename(tableStatus.getPath(), newPath);
		}
		
		tables = fs.listStatus(new Path(tablesDir));
		
		for (FileStatus tableStatus : tables) {
			FileStatus[] tablets = fs.listStatus(tableStatus.getPath());
			for (FileStatus tabletStatus : tablets) {
				FileStatus[] files = fs.listStatus(tabletStatus.getPath());
				for (FileStatus fileStatus : files) {
					String name = fileStatus.getPath().getName(); 
					if(name.matches("map_\\d+_\\d+")){
						fs.rename(fileStatus.getPath(), new Path(fileStatus.getPath().getParent(), name.substring(4)+".map"));
					}
				}
				
			}
		}
		
	}
}
