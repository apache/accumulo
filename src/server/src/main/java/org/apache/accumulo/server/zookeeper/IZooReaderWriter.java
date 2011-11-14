package org.apache.accumulo.server.zookeeper;

import java.util.List;

import org.apache.accumulo.core.zookeeper.IZooReader;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter.Mutator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

public interface IZooReaderWriter extends IZooReader {
  
  public abstract ZooKeeper getZooKeeper();
  
  public abstract void recursiveDelete(String zPath, NodeMissingPolicy policy) throws KeeperException, InterruptedException;
  
  public abstract void recursiveDelete(String zPath, int version, NodeMissingPolicy policy) throws KeeperException, InterruptedException;
  
  /**
   * Create a persistent node with the default ACL
   * 
   * @return true if the node was created or altered; false if it was skipped
   */
  public abstract boolean putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException;
  
  public abstract boolean putPrivatePersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException;
  
  public abstract void putPersistentData(String zPath, byte[] data, int version, NodeExistsPolicy policy) throws KeeperException, InterruptedException;
  
  public abstract String putPersistentSequential(String zPath, byte[] data) throws KeeperException, InterruptedException;
  
  public abstract String putEphemeralSequential(String zPath, byte[] data) throws KeeperException, InterruptedException;
  
  public abstract void recursiveCopyPersistent(String source, String destination, NodeExistsPolicy policy) throws KeeperException, InterruptedException;
  
  public abstract void delete(String path, int version) throws InterruptedException, KeeperException;
  
  public abstract byte[] mutate(String zPath, byte[] createValue, List<ACL> acl, Mutator mutator) throws Exception;
  
  public abstract boolean isLockHeld(ZooUtil.LockID lockID) throws KeeperException, InterruptedException;
  
  public abstract void mkdirs(String path) throws KeeperException, InterruptedException;
  
}
