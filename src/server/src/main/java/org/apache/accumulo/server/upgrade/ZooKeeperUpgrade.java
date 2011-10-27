/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.upgrade;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.master.state.tables.TableManager;
import org.apache.accumulo.server.master.state.tables.TableState;
import org.apache.accumulo.server.util.Initialize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperUpgrade extends Initialize {
  private static final Logger log = Logger.getLogger(ZooKeeperUpgrade.class);
  private static final String OLD_MASTER_LOCK = "/master_lock";
  private static final String OLD_ROOT_TABLET_LOGS = "/root_tablet_logs";
  private static final String OLD_ROOT_TABLET_LOC = "/root_tablet_loc";
  private static final String OLD_CONF_DIR = "/conf";
  private static final String OLD_TABLE_CONF_DIR = OLD_CONF_DIR + "/tables";
  
  private static String zkInstanceRoot;
  
  public static void main(String[] args) {
    try {
      zkInstanceRoot = ZooUtil.getRoot(HdfsZooInstance.getInstance());
      upgradeZooKeeper();
      upgradeSecurity();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
  
  static void upgradeSecurity() throws Exception {
    
    SortedMap<String,String> tableIds = Tables.getNameToIdMap(HdfsZooInstance.getInstance());
    
    String ZKUserPath = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + "/users";
    
    ZooKeeper zk = ZooSession.getSession();
    
    List<String> users = zk.getChildren(ZKUserPath, false);
    
    for (String user : users) {
      if (zk.exists(ZKUserPath + "/" + user + "/Tables", false) != null) {
        move(ZKUserPath + "/" + user + "/Tables", ZKUserPath + "/" + user + "/Tables.old");
        zk.create(ZKUserPath + "/" + user + "/Tables", new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        List<String> tables = zk.getChildren(ZKUserPath + "/" + user + "/Tables.old", false);
        for (String table : tables) {
          if (tableIds.containsKey(table))
            move(ZKUserPath + "/" + user + "/Tables.old/" + table, ZKUserPath + "/" + user + "/Tables/" + tableIds.get(table));
          else
            System.err.println("WARN : User " + user + " has permissions for non-existant table " + table);
        }
        
        ZooUtil.recursiveDelete(ZKUserPath + "/" + user + "/Tables.old", NodeMissingPolicy.SKIP);
      }
    }
    
    // ZKUserPath+"/"+user+ZKUserTablePerms+"/"+table
    
  }
  
  static void upgradeZooKeeper() throws IOException, KeeperException, InterruptedException {
    SortedSet<String> tableNames = getOldTableNames();
    
    // initialize zookeeper layout (skip 1.1 dirs that may already be there)
    ZooUtil.putPersistentData(zkInstanceRoot + Constants.ZLOGGERS, new byte[0], NodeExistsPolicy.SKIP);
    ZooUtil.putPersistentData(zkInstanceRoot + Constants.ZPROBLEMS, new byte[0], NodeExistsPolicy.SKIP);
    ZooUtil.putPersistentData(zkInstanceRoot + Constants.ZTSERVERS, new byte[0], NodeExistsPolicy.SKIP);
    ZooUtil.putPersistentData(zkInstanceRoot + Constants.ZGC, new byte[0], NodeExistsPolicy.SKIP);
    ZooUtil.putPersistentData(zkInstanceRoot + Constants.ZGC_LOCK, new byte[0], NodeExistsPolicy.SKIP);
    ZooUtil.putPersistentData(zkInstanceRoot + Constants.ZROOT_TABLET, new byte[0], NodeExistsPolicy.FAIL);
    move(zkInstanceRoot + OLD_ROOT_TABLET_LOGS, zkInstanceRoot + Constants.ZROOT_TABLET_WALOGS);
    move(zkInstanceRoot + OLD_ROOT_TABLET_LOC, zkInstanceRoot + Constants.ZROOT_TABLET_LOCATION);
    ZooUtil.putPersistentData(zkInstanceRoot + Constants.ZMASTERS, new byte[0], NodeExistsPolicy.FAIL);
    move(zkInstanceRoot + OLD_MASTER_LOCK, zkInstanceRoot + Constants.ZMASTER_LOCK);
    ZooUtil.putPersistentData(zkInstanceRoot + Constants.ZTABLES, BigInteger.valueOf(tableNames.size() - 1).toString(Character.MAX_RADIX).getBytes(),
        NodeExistsPolicy.FAIL);
    
    // initialize tables
    BigInteger bigInt = new BigInteger(new String(Constants.ZTABLES_INITIAL_ID), Character.MAX_RADIX);
    
    // allocate table ids in same sort order as table names (this avoids 00 padding table ids)
    SortedSet<String> tableIds = new TreeSet<String>();
    for (String tableName : tableNames) {
      if (!tableName.equals(Constants.METADATA_TABLE_NAME)) {
        String tableId = bigInt.toString(Character.MAX_RADIX);
        bigInt = bigInt.add(BigInteger.ONE);
        tableIds.add(tableId);
      }
    }
    
    Iterator<String> tii = tableIds.iterator();
    
    for (String tableName : tableNames) {
      System.out.println(tableName + " is being upgraded");
      
      String tableId;
      if (!tableName.equals(Constants.METADATA_TABLE_NAME))
        tableId = tii.next();
      else
        tableId = Constants.METADATA_TABLE_ID;
      
      TableManager.prepareNewTableState(HdfsZooInstance.getInstance().getInstanceID(), tableId, tableName, TableState.OFFLINE);
      String oldConfDir = zkInstanceRoot + OLD_TABLE_CONF_DIR + "/" + tableName;
      if (ZooUtil.exists(oldConfDir))
        if (tableId.equals(Constants.METADATA_TABLE_ID))
          ZooUtil.recursiveDelete(oldConfDir, NodeMissingPolicy.SKIP);
        else
          ZooUtil.recursiveCopyPersistent(oldConfDir, zkInstanceRoot + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_CONF, NodeExistsPolicy.OVERWRITE);
      
      if (!tableId.equals(Constants.METADATA_TABLE_ID))
        validateConfig(tableId);
      else
        initMetadataConfig();
    }
    
    // clean up old table configuration directories
    ZooUtil.recursiveDelete(zkInstanceRoot + OLD_CONF_DIR, NodeMissingPolicy.SKIP);
  }
  
  private static SortedSet<String> getOldTableNames() throws IOException, KeeperException, InterruptedException {
    SortedSet<String> tableNames = new TreeSet<String>();
    // get list of 1.1 tables from HDFS and ZooKeeper
    FileStatus[] tablesStatus = FileSystem.get(new Configuration()).listStatus(new Path(Constants.getTablesDir()));
    for (FileStatus fstat : tablesStatus) {
      String tableName = fstat.getPath().toString();
      tableName = tableName.substring(tableName.lastIndexOf('/') + 1);
      tableNames.add(tableName);
    }
    if (ZooUtil.exists(zkInstanceRoot + OLD_TABLE_CONF_DIR))
      tableNames.addAll(ZooSession.getSession().getChildren(zkInstanceRoot + OLD_TABLE_CONF_DIR, false));
    return tableNames;
  }
  
  private static void move(String source, String destination) throws KeeperException, InterruptedException {
    if (ZooUtil.exists(source)) {
      ZooUtil.recursiveCopyPersistent(source, destination, NodeExistsPolicy.FAIL);
      ZooUtil.recursiveDelete(source, NodeMissingPolicy.SKIP);
    }
  }
  
  private static void validateConfig(String tableId) throws KeeperException, InterruptedException {
    String confPath = zkInstanceRoot + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_CONF;
    ZooKeeper zk = ZooSession.getSession();
    for (String propKey : zk.getChildren(confPath, false)) {
      String propKeyPath = confPath + "/" + propKey;
      if (!Property.isValidTablePropertyKey(propKey)) {
        log.warn("Removing invalid per-table property: " + propKey);
        ZooUtil.recursiveDelete(zk, propKeyPath, NodeMissingPolicy.FAIL);
      }
    }
  }
}
