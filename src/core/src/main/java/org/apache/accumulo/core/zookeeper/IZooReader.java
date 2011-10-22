package org.apache.accumulo.core.zookeeper;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public interface IZooReader {
  
  public abstract byte[] getData(String zPath, Stat stat) throws KeeperException, InterruptedException;
  
  public abstract Stat getStatus(String zPath) throws KeeperException, InterruptedException;
  
  public abstract Stat getStatus(String zPath, Watcher watcher) throws KeeperException, InterruptedException;
  
  public abstract List<String> getChildren(String zPath) throws KeeperException, InterruptedException;
  
  public abstract List<String> getChildren(String zPath, Watcher watcher) throws KeeperException, InterruptedException;
  
  public abstract boolean exists(String zPath) throws KeeperException, InterruptedException;
  
  public abstract boolean exists(String zPath, Watcher watcher) throws KeeperException, InterruptedException;
  
}