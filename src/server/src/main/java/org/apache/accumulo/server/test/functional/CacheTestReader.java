package org.apache.accumulo.server.test.functional;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooCache;


public class CacheTestReader {
	public static void main(String[] args) throws Exception {
		String rootDir = args[0];
		String reportDir = args[1];
		String keepers = args[2];
		int numData = CacheTestWriter.NUM_DATA;
		
		File myfile = new File(reportDir+"/"+UUID.randomUUID());
		myfile.deleteOnExit();
		
		ZooCache zc = new ZooCache(keepers, 30000);
		
		while(true){
			if(myfile.exists())
				myfile.delete();
			
			if(zc.get(rootDir+"/die") != null){
				return;
			}
			
			Map<String, String> readData = new TreeMap<String, String>();
			
			for(int i = 0; i < numData; i++){
				byte[] v = zc.get(rootDir+"/data"+i);
				if(v != null)
					readData.put(rootDir+"/data"+i, new String(v));
			}

			byte[] v = zc.get(rootDir+"/dataS");
			if(v != null)
				readData.put(rootDir+"/dataS", new String(v));
			
			List<String> children = zc.getChildren(rootDir+"/dir");
			if(children != null)
				for (String child : children) {
					readData.put(rootDir+"/dir/"+child, "");
				}
			
			FileOutputStream fos = new FileOutputStream(myfile);
			ObjectOutputStream oos = new ObjectOutputStream(fos);

			oos.writeObject(readData);

			oos.close();
			
			
			UtilWaitThread.sleep(20);
		}
		
	}
}
