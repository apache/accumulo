package org.apache.accumulo.server.zookeeper;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.zookeeper.Watcher;


public class ZooCache extends org.apache.accumulo.core.zookeeper.ZooCache {
    public ZooCache() {
        this(null);
    }
    
    public ZooCache(Watcher watcher) {
        super(ZooReaderWriter.getInstance(), watcher);
    }
    
    public ZooCache(AccumuloConfiguration conf, Watcher watcher) {
        super(conf.get(Property.INSTANCE_ZK_HOST),
                (int)conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT),
                watcher);
    }
}
