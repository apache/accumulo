package org.apache.accumulo.server.util;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.zookeeper.ZooKeeper;


public class DeleteZooInstance {

    /**
     * @param args : the name or UUID of the instance to be deleted
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: " + DeleteZooInstance.class.getName() + " [instanceName|UUID ... ]");
            System.exit(1);
        }
        String keepers = AccumuloConfiguration.getSystemConfiguration().get(Property.INSTANCE_ZK_HOST);
        ZooKeeper zk = ZooSession.getSession(keepers, 30000);
        // try instance name:
        Set<String> instances = new HashSet<String>(zk.getChildren(Constants.ZROOT+Constants.ZINSTANCES, null));
        Set<String> uuids = new HashSet<String>(zk.getChildren(Constants.ZROOT, null));
        uuids.remove("instances");
        for (String name : args) {
            if (instances.contains(args[0])) {
                String path = Constants.ZROOT + Constants.ZINSTANCES + "/" + name;
                byte[] data = zk.getData(path, null, null);
                CleanZookeeper.recursivelyDelete(zk, path);
                CleanZookeeper.recursivelyDelete(zk, Constants.ZROOT + "/" + new String(data));
            }
            else if (uuids.contains(name)) {
                // look for the real instance name
                for (String instance : instances) {
                    String path = Constants.ZROOT + Constants.ZINSTANCES + "/" + instance;
                    byte[] data = zk.getData(path, null, null);
                    if (name.equals(new String(data)))
                        zk.delete(path, -1);
                }
                CleanZookeeper.recursivelyDelete(zk, Constants.ZROOT + "/" + name);
            }
        }
    }

}
