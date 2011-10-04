package org.apache.accumulo.server.master.state;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;


public class DeadServerList {
    private static final Logger log = Logger.getLogger(DeadServerList.class);
    private final String path;
    
    public DeadServerList(String path) {
        this.path = path;
        ZooReaderWriter zoo = ZooReaderWriter.getInstance();
        try {
            zoo.mkdirs(path);
        } catch (Exception ex) {
            log.error("Unable to make parent directories of " + path, ex);
        }
    }

    public List<DeadServer> getList() {
        List<DeadServer> result = new ArrayList<DeadServer>();
        ZooReaderWriter zoo = ZooReaderWriter.getInstance();
        try {
            List<String> children = zoo.getChildren(path);
            if (children != null) {
                for (String child : children) {
                    Stat stat = new Stat();
                    byte[] data = zoo.getData(path + "/" + child, stat);
                    DeadServer server = new DeadServer(child, stat.getMtime(), new String(data));
                    result.add(server);
                }
            }
        } catch (Exception ex) {
            log.error(ex, ex);
        }
        return result;
    }
    
    public void delete(String server) {
        ZooReaderWriter zoo = ZooReaderWriter.getInstance();
        try {
            zoo.recursiveDelete(path + "/" + server, NodeMissingPolicy.SKIP);
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }
    
    public void post(String server, String cause) {
        ZooReaderWriter zoo = ZooReaderWriter.getInstance();
        try {
            zoo.putPersistentData(path + "/" + server, cause.getBytes(), NodeExistsPolicy.SKIP);
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }
}
