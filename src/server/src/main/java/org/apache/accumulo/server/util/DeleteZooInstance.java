package org.apache.accumulo.server.util;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;



public class DeleteZooInstance {

    /**
     * @param args : the name or UUID of the instance to be deleted
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: " + DeleteZooInstance.class.getName() + " [instanceName|UUID ... ]");
            System.exit(1);
        }
        ZooReaderWriter zk = ZooReaderWriter.getInstance();
        // try instance name:
        Set<String> instances = new HashSet<String>(zk.getChildren(Constants.ZROOT+Constants.ZINSTANCES));
        Set<String> uuids = new HashSet<String>(zk.getChildren(Constants.ZROOT));
        uuids.remove("instances");
        for (String name : args) {
            if (instances.contains(args[0])) {
                String path = Constants.ZROOT + Constants.ZINSTANCES + "/" + name;
                byte[] data = zk.getData(path, null);
                zk.recursiveDelete(path, NodeMissingPolicy.SKIP);
                zk.recursiveDelete(Constants.ZROOT + "/" + new String(data), NodeMissingPolicy.SKIP);
            }
            else if (uuids.contains(name)) {
                // look for the real instance name
                for (String instance : instances) {
                    String path = Constants.ZROOT + Constants.ZINSTANCES + "/" + instance;
                    byte[] data = zk.getData(path, null);
                    if (name.equals(new String(data)))
                        zk.recursiveDelete(path, NodeMissingPolicy.SKIP);
                }
                zk.recursiveDelete(Constants.ZROOT + "/" + name, NodeMissingPolicy.SKIP);
            }
        }
    }

}
