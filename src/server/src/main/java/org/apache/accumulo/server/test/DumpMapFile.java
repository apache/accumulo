package org.apache.accumulo.server.test;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.log4j.Logger;




public class DumpMapFile {
	private static final Logger log = Logger.getLogger(DumpMapFile.class);
	
	public static void main(String[] args) {
		try {
			Configuration conf = CachedConfiguration.getInstance();
			FileSystem fs = FileSystem.get(conf);
			
			MyMapFile.Reader mr = new MyMapFile.Reader(fs, args[0], conf);
			Key key = new Key();
			Value value = new Value();
			
			long start = System.currentTimeMillis();
			while(mr.next(key, value)) {
			    log.info(key + " -> " + value);
			}
			long stop = System.currentTimeMillis();
			log.info(stop - start);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
