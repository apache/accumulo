package org.apache.accumulo.core.file.rfile;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class CreateCompatTestFile {
	
	public static Set<ByteSequence> ncfs(String... colFams) {
		HashSet<ByteSequence> cfs = new HashSet<ByteSequence>();
		
		for (String cf : colFams) {
			cfs.add(new ArrayByteSequence(cf));
		}
		
		return cfs;
	}
	
	private static Key nk(String row, String cf, String cq, String cv, long ts) {
		return new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), cv.getBytes(), ts);
	}
	
	private static Value nv(String val) {
		return new Value(val.getBytes());
	}
	
	private static String nf(String prefix, int i) {
		return String.format(prefix+"%06d",i);
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		CachableBlockFile.Writer _cbw = new CachableBlockFile.Writer(fs,new Path(args[0]),"gz",conf);
		RFile.Writer writer = new RFile.Writer(_cbw, 1000);
		
		writer.startNewLocalityGroup("lg1", ncfs(nf("cf_", 1), nf("cf_", 2)));
		
		for(int i = 0; i < 1000; i++){
			writer.append(nk(nf("r_",i), nf("cf_",1), nf("cq_",0), "", 1000-i), nv(i+""));
			writer.append(nk(nf("r_",i), nf("cf_",2), nf("cq_",0), "", 1000-i), nv(i+""));
		}
		
		writer.startNewLocalityGroup("lg2", ncfs(nf("cf_", 3)));
		
		for(int i = 0; i < 1000; i++){
			writer.append(nk(nf("r_",i), nf("cf_",3), nf("cq_",0), "", 1000-i), nv(i+""));
		}
		
		writer.startDefaultLocalityGroup();
		
		for(int i = 0; i < 1000; i++){
			writer.append(nk(nf("r_",i), nf("cf_",4), nf("cq_",0), "", 1000-i), nv(i+""));
		}
		
		writer.close();
		_cbw.close();
	}
}
