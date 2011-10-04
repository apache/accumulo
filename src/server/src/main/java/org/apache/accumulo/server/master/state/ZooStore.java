package org.apache.accumulo.server.master.state;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.log4j.Logger;


public class ZooStore implements DistributedStore {
    
    private static final Logger log = Logger.getLogger(ZooStore.class);

    String basePath;
    
    ZooCache cache = new ZooCache();
    
    public ZooStore(String basePath) throws IOException {
        this.basePath = basePath;
        if (basePath.endsWith("/"))
            basePath.substring(0, basePath.length() - 1);
    }
    
    public ZooStore() throws IOException {
        this(ZooUtil.getRoot(HdfsZooInstance.getInstance().getInstanceID()));
    }

    @Override
    public byte[] get(String path) throws DistributedStoreException {
        try {
            return cache.get(relative(path));
        } catch (Exception ex) {
            throw new DistributedStoreException(ex);
        }
    }

    private String relative(String path) {
        return basePath + path;
    }


    @Override
    public List<String> getChildren(String path) throws DistributedStoreException {
        try {
            return cache.getChildren(relative(path));
        } catch (Exception ex) {
            throw new DistributedStoreException(ex);
        }
    }

    @Override
    public void put(String path, byte[] bs) throws DistributedStoreException {
        try {
            path = relative(path);
            ZooUtil.putPersistentData(path, bs, NodeExistsPolicy.OVERWRITE);
            cache.clear();
            log.debug("Wrote " + new String(bs) + " to " + path);
        } catch (Exception ex) {
            throw new DistributedStoreException(ex);
        }
    }

    @Override
    public void remove(String path) throws DistributedStoreException {
        try {
            log.debug("Removing " + path);
            path = relative(path);
            if (ZooUtil.exists(path))
                ZooUtil.recursiveDelete(path, NodeMissingPolicy.SKIP);
            cache.clear();
        } catch (Exception ex) {
            throw new DistributedStoreException(ex);
        }
    }
}
