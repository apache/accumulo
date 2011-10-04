package org.apache.accumulo.server.util;

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


public class CleanZookeeper {

    static void recursivelyDelete(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        for (String child : zk.getChildren(path, false) ) {
            recursivelyDelete(zk, path + "/" + child);
        }
        zk.delete(path, -1);
    }
    
    /**
     * @param args should contain one element: the address of a zookeeper node
     * @throws IOException error connecting to accumulo or zookeeper
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: " + CleanZookeeper.class.getName() + " hostname[:port]");
            System.exit(1);
        }
        String root = Constants.ZROOT;
        ZooKeeper zk = new ZooKeeper(args[0], 10000, new Watcher() {
            public void process(WatchedEvent event) {}
        });
        try {
            for (String child : zk.getChildren(root, false) ) {
            	if (Constants.ZINSTANCES.equals("/"+child))
            	{
            		for (String instanceName : zk.getChildren(root+Constants.ZINSTANCES, false))
            		{
            			String instanceNamePath = root+Constants.ZINSTANCES+"/"+instanceName;
            			byte[] id = zk.getData(instanceNamePath, false, null);
            			if (id != null && !new String(id).equals(HdfsZooInstance.getInstance().getInstanceID()))
            				recursivelyDelete(zk, instanceNamePath);
            		}
            	}
            	else if (!child.equals(HdfsZooInstance.getInstance().getInstanceID()))
            	{
                    recursivelyDelete(zk, root + "/" + child);
                }
            }
        } catch (Exception ex) {
            System.out.println("Error Occurred: " + ex);
        }
    }

}
