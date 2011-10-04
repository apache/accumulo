package org.apache.accumulo.server.master.state;

import java.util.List;

/*
 * An abstract version of ZooKeeper that we can write tests against.
 */
public interface DistributedStore {

    public List<String> getChildren(String path) throws DistributedStoreException;

    public byte[] get(String path) throws DistributedStoreException;

    public void put(String path, byte[] bs) throws DistributedStoreException;

    public void remove(String path) throws DistributedStoreException;

}
