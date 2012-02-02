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
package org.apache.accumulo.server.master.tableOps;

import java.io.IOException;
import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.fate.Repo;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.master.state.tables.TableManager;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.security.ZKAuthenticator;
import org.apache.accumulo.server.util.MetadataTable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

class CleanUp extends MasterRepo {
  
  final private static Logger log = Logger.getLogger(CleanUp.class);
  
  private static final long serialVersionUID = 1L;
  
  private String tableId;
  
  private long creationTime;
  
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    
    /*
     * handle the case where we start executing on a new machine where the current time is in the past relative to the previous machine
     * 
     * if the new machine has time in the future, that will work ok w/ hasCycled
     */
    if (System.currentTimeMillis() < creationTime) {
      creationTime = System.currentTimeMillis();
    }
    
  }
  
  public CleanUp(String tableId) {
    this.tableId = tableId;
    creationTime = System.currentTimeMillis();
  }
  
  @Override
  public long isReady(long tid, Master environment) throws Exception {
    if (!environment.hasCycled(creationTime)) {
      return 50;
    }
    
    boolean done = true;
    Range tableRange = new KeyExtent(new Text(tableId), null, null).toMetadataRange();
    Scanner scanner = environment.getInstance().getConnector(SecurityConstants.getSystemCredentials())
        .createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
    MetaDataTableScanner.configureScanner(scanner, environment);
    scanner.setRange(tableRange);
    
    KeyExtent prevExtent = null;
    for (Entry<Key,Value> entry : scanner) {
      TabletLocationState locationState = MetaDataTableScanner.createTabletLocationState(entry.getKey(), entry.getValue());
      if (!locationState.extent.isPreviousExtent(prevExtent)) {
        log.debug("Still waiting for table to be deleted: " + tableId + " saw inconsistency" + prevExtent + " " + locationState.extent);
        done = false;
        break;
      }
      prevExtent = locationState.extent;
      
      TabletState state = locationState.getState(environment.onlineTabletServers());
      if (state.equals(TabletState.ASSIGNED) || state.equals(TabletState.HOSTED)) {
        log.debug("Still waiting for table to be deleted: " + tableId + " locationState: " + locationState);
        done = false;
        break;
      }
    }

    if (!done)
      return 50;
    
    return 0;
  }
  
  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    
    Instance instance = HdfsZooInstance.getInstance();
    
    environment.clearMigrations(tableId);
    
    int refCount = 0;
    
    try {
      // look for other tables that references this tables files
      Connector conn = instance.getConnector(SecurityConstants.getSystemCredentials());
      BatchScanner bs = conn.createBatchScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS, 8);
      try {
        bs.setRanges(Collections.singleton(Constants.NON_ROOT_METADATA_KEYSPACE));
        bs.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
        IteratorSetting cfg = new IteratorSetting(40, "grep", GrepIterator.class);
        GrepIterator.setTerm(cfg, "../" + tableId + "/");
        bs.addScanIterator(cfg);
        
        for (Entry<Key,Value> entry : bs) {
          if (entry.getKey().getColumnQualifier().toString().startsWith("../" + tableId + "/")) {
            refCount++;
          }
        }
      } finally {
        bs.close();
      }
      
    } catch (Exception e) {
      refCount = -1;
      log.error("Failed to scan !METADATA looking for references to deleted table " + tableId, e);
    }
    
    // remove metadata table entries
    try {
      // Intentionally do not pass master lock. If master loses lock, this operation may complete before master can kill itself.
      // If the master lock passed to deleteTable, it is possible that the delete mutations will be dropped. If the delete operations
      // are dropped and the operation completes, then the deletes will not be repeated.
      MetadataTable.deleteTable(tableId, refCount != 0, SecurityConstants.getSystemCredentials(), null);
    } catch (Exception e) {
      log.error("error deleting " + tableId + " from metadata table", e);
    }
    
    // remove any problem reports the table may have
    try {
      ProblemReports.getInstance().deleteProblemReports(tableId);
    } catch (Exception e) {
      log.error("Failed to delete problem reports for table " + tableId, e);
    }
    
    if (refCount == 0) {
      // delete the map files
      try {
        FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
        fs.delete(new Path(ServerConstants.getTablesDir(), tableId), true);
      } catch (IOException e) {
        log.error("Unable to remove deleted table directory", e);
      }
    }
    
    // remove table from zookeeper
    try {
      TableManager.getInstance().removeTable(tableId);
      Tables.clearCache(instance);
    } catch (Exception e) {
      log.error("Failed to find table id in zookeeper", e);
    }
    
    // remove any permissions associated with this table
    try {
      ZKAuthenticator.getInstance().deleteTable(SecurityConstants.getSystemCredentials(), tableId);
    } catch (AccumuloSecurityException e) {
      log.error(e.getMessage(), e);
    }
    
    Utils.unreserveTable(tableId, tid, true);
    
    Logger.getLogger(CleanUp.class).debug("Deleted table " + tableId);
    
    return null;
  }
  
  @Override
  public void undo(long tid, Master environment) throws Exception {
    // nothing to do
  }
  
}

public class DeleteTable extends MasterRepo {
  
  private static final long serialVersionUID = 1L;
  
  private String tableId;
  
  public DeleteTable(String tableId) {
    this.tableId = tableId;
  }
  
  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveTable(tableId, tid, true, true, TableOperation.DELETE);
  }
  
  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    TableManager.getInstance().transitionTableState(tableId, TableState.DELETING);
    environment.getEventCoordinator().event("deleting table %s ", tableId);
    return new CleanUp(tableId);
  }
  
  @Override
  public void undo(long tid, Master environment) throws Exception {
    Utils.unreserveTable(tableId, tid, true);
  }
  
}
