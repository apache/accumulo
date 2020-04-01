/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.tableOps.delete;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CleanUp extends MasterRepo {

  private static final Logger log = LoggerFactory.getLogger(CleanUp.class);

  private static final long serialVersionUID = 1L;

  private TableId tableId;
  private NamespaceId namespaceId;

  private long creationTime;

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();

    // handle the case where we start executing on a new machine where the current time is in the
    // past relative to the previous machine
    // if the new machine has time in the future, that will work ok w/ hasCycled
    if (System.currentTimeMillis() < creationTime) {
      creationTime = System.currentTimeMillis();
    }

  }

  public CleanUp(TableId tableId, NamespaceId namespaceId) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    creationTime = System.currentTimeMillis();
  }

  @Override
  public long isReady(long tid, Master master) throws Exception {
    if (!master.hasCycled(creationTime)) {
      return 50;
    }

    boolean done = true;
    Range tableRange = new KeyExtent(tableId, null, null).toMetadataRange();
    Scanner scanner = master.getContext().createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    MetaDataTableScanner.configureScanner(scanner, master);
    scanner.setRange(tableRange);

    for (Entry<Key,Value> entry : scanner) {
      TabletLocationState locationState =
          MetaDataTableScanner.createTabletLocationState(entry.getKey(), entry.getValue());
      TabletState state = locationState.getState(master.onlineTabletServers());
      if (!state.equals(TabletState.UNASSIGNED)) {
        // This code will even wait on tablets that are assigned to dead tablets servers. This is
        // intentional because the master may make metadata writes for these tablets. See #587
        log.debug("Still waiting for table to be deleted: " + tableId + " locationState: "
            + locationState);
        done = false;
        break;
      }
    }

    if (!done)
      return 50;

    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master master) {

    master.clearMigrations(tableId);

    int refCount = 0;

    try {
      // look for other tables that references this table's files
      AccumuloClient client = master.getContext();
      try (BatchScanner bs =
          client.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 8)) {
        Range allTables = MetadataSchema.TabletsSection.getRange();
        Range tableRange = MetadataSchema.TabletsSection.getRange(tableId);
        Range beforeTable =
            new Range(allTables.getStartKey(), true, tableRange.getStartKey(), false);
        Range afterTable = new Range(tableRange.getEndKey(), false, allTables.getEndKey(), true);
        bs.setRanges(Arrays.asList(beforeTable, afterTable));
        bs.fetchColumnFamily(DataFileColumnFamily.NAME);
        IteratorSetting cfg = new IteratorSetting(40, "grep", GrepIterator.class);
        GrepIterator.setTerm(cfg, "/" + tableId + "/");
        bs.addScanIterator(cfg);

        for (Entry<Key,Value> entry : bs) {
          if (entry.getKey().getColumnQualifier().toString().contains("/" + tableId + "/")) {
            refCount++;
          }
        }
      }

    } catch (Exception e) {
      refCount = -1;
      log.error("Failed to scan " + MetadataTable.NAME + " looking for references to deleted table "
          + tableId, e);
    }

    // remove metadata table entries
    try {
      // Intentionally do not pass master lock. If master loses lock, this operation may complete
      // before master can kill itself.
      // If the master lock passed to deleteTable, it is possible that the delete mutations will be
      // dropped. If the delete operations
      // are dropped and the operation completes, then the deletes will not be repeated.
      MetadataTableUtil.deleteTable(tableId, refCount != 0, master.getContext(), null);
    } catch (Exception e) {
      log.error("error deleting " + tableId + " from metadata table", e);
    }

    // remove any problem reports the table may have
    try {
      ProblemReports.getInstance(master.getContext()).deleteProblemReports(tableId);
    } catch (Exception e) {
      log.error("Failed to delete problem reports for table " + tableId, e);
    }

    if (refCount == 0) {
      // delete the map files
      try {
        VolumeManager fs = master.getVolumeManager();
        for (String dir : ServerConstants.getTablesDirs(master.getContext())) {
          fs.deleteRecursively(new Path(dir, tableId.canonical()));
        }
      } catch (IOException e) {
        log.error("Unable to remove deleted table directory", e);
      } catch (IllegalArgumentException exception) {
        if (exception.getCause() instanceof UnknownHostException) {
          /* Thrown if HDFS encounters a DNS problem in some edge cases */
          log.error("Unable to remove deleted table directory", exception);
        } else {
          throw exception;
        }
      }
    }

    // remove table from zookeeper
    try {
      master.getTableManager().removeTable(tableId);
      Tables.clearCache(master.getContext());
    } catch (Exception e) {
      log.error("Failed to find table id in zookeeper", e);
    }

    // remove any permissions associated with this table
    try {
      AuditedSecurityOperation.getInstance(master.getContext())
          .deleteTable(master.getContext().rpcCreds(), tableId, namespaceId);
    } catch (ThriftSecurityException e) {
      log.error("{}", e.getMessage(), e);
    }

    Utils.unreserveTable(master, tableId, tid, true);
    Utils.unreserveNamespace(master, namespaceId, tid, false);

    LoggerFactory.getLogger(CleanUp.class).debug("Deleted table " + tableId);

    return null;
  }

  @Override
  public void undo(long tid, Master environment) {
    // nothing to do
  }

}
