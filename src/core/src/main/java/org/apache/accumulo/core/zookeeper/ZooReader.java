package org.apache.accumulo.core.zookeeper;

import java.util.List;

import org.apache.accumulo.core.client.Instance;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class ZooReader {
    
    protected String keepers;
    protected int timeout;
    
    protected ZooKeeper getSession(String keepers, int timeout, String auth) {
        return ZooSession.getSession(keepers, timeout, auth);
    }
    
    protected ZooKeeper getZooKeeper() {
        return getSession(keepers, timeout, null);
    }
    
    public byte[] getData(String zPath, Stat stat) throws KeeperException, InterruptedException
    {
        return getZooKeeper().getData(zPath, false, stat);
    }

    public Stat getStatus(String zPath) throws KeeperException, InterruptedException
    {
        return getZooKeeper().exists(zPath, false);
    }
    
    public Stat getStatus(String zPath, Watcher watcher) throws KeeperException, InterruptedException
    {
        return getZooKeeper().exists(zPath, watcher);
    }
    
    public List<String> getChildren(String zPath) throws KeeperException, InterruptedException
    {
        return getZooKeeper().getChildren(zPath, false);
    }

    public List<String> getChildren(String zPath, Watcher watcher) throws KeeperException, InterruptedException
    {
        return getZooKeeper().getChildren(zPath, watcher);
    }

    public boolean exists(String zPath) throws KeeperException, InterruptedException
    {
        return getZooKeeper().exists(zPath, false) != null;
    }
    
    public boolean exists(String zPath, Watcher watcher) throws KeeperException, InterruptedException
    {
        return getZooKeeper().exists(zPath, watcher) != null;
    }
    
    public ZooReader(String keepers, int timeout) {
        this.keepers = keepers;
        this.timeout = timeout;
    }

    public ZooReader(Instance instance) {
        this(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    }
}
