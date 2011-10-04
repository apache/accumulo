package org.apache.accumulo.server.util;

import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.zookeeper.ZooLock;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.zookeeper.ZooKeeper;


public class TabletServerLocks {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		
		String tserverPath = ZooUtil.getRoot(HdfsZooInstance.getInstance())+Constants.ZTSERVERS;
		
		if(args.length == 1 && args[0].equals("-list")){
			ZooKeeper session = ZooSession.getSession();
			
			List<String> tabletServers = session.getChildren(tserverPath, false);
			
			for (String tabletServer : tabletServers) {
				byte[] lockData = ZooLock.getLockData(tserverPath+"/"+tabletServer);
				String holder = null;
				if(lockData != null){
					holder = new String(lockData);
				}
				
				System.out.printf("%32s %16s\n", tabletServer, holder);
			}
		}else if(args.length == 2 && args[0].equals("-delete")){
			ZooLock.deleteLock(tserverPath+"/"+args[1]);
		}else{
			System.out.println("Usage : "+TabletServerLocks.class.getName()+" -list|-delete <tserver lock>");
		}
		
	}

}
