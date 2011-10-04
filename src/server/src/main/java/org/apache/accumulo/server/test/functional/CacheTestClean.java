package org.apache.accumulo.server.test.functional;

import java.io.File;

import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.ZooKeeper;


public class CacheTestClean {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String rootDir = args[0];
		File reportDir = new File(args[1]);
		
		ZooKeeper zk = ZooSession.getSession();
		
		if(zk.exists(rootDir, false) != null){
			ZooUtil.recursiveDelete(rootDir, NodeMissingPolicy.FAIL);
		}

		if(!reportDir.exists()){
			reportDir.mkdir();
		}else{
			File[] files = reportDir.listFiles();
			if(files.length != 0)
				throw new Exception("dir "+reportDir+" is not empty");
		}
		
	}

}
