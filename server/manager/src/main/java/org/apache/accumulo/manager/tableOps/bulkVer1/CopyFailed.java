/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.manager.tableOps.bulkVer1;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.ValidationUtil;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CopyFailed extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(CopyFailed.class);

  private static final long serialVersionUID = 1L;

  private TableId tableId;
  private String source;
  private String bulk;
  private String error;

  public CopyFailed(TableId tableId, String source, String bulk, String error) {
    this.tableId = tableId;
    this.source = source;
    this.bulk = bulk;
    this.error = error;
  }

  @Override
  public long isReady(long tid, Manager manager) {
    Set<TServerInstance> finished = new HashSet<>();
    Set<TServerInstance> running = manager.onlineTabletServers();
    for (TServerInstance server : running) {
      try {
        TServerConnection client = manager.getConnection(server);
        if (client != null && !client.isActive(tid)) {
          finished.add(server);
        }
      } catch (TException ex) {
        log.info("Ignoring error trying to check on tid " + FateTxId.formatTid(tid)
            + " from server " + server + ": " + ex);
      }
    }
    if (finished.containsAll(running)) {
      return 0;
    }
    return 500;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    // This needs to execute after the arbiter is stopped
    manager.updateBulkImportStatus(source, BulkImportState.COPY_FILES);
    VolumeManager fs = manager.getVolumeManager();

    if (!fs.exists(new Path(error, BulkImport.FAILURES_TXT))) {
      return new CleanUpBulkImport(tableId, source, bulk, error);
    }

    var failures = new HashSet<Path>();
    var loadedFailures = new HashSet<Path>();

    try (BufferedReader in = new BufferedReader(
        new InputStreamReader(fs.open(new Path(error, BulkImport.FAILURES_TXT)), UTF_8))) {
      String line = null;
      while ((line = in.readLine()) != null) {
        Path path = new Path(line);
        if (!fs.exists(new Path(error, path.getName()))) {
          failures.add(path);
        }
      }
    }

    /*
     * I thought I could move files that have no file references in the table. However its possible
     * a clone references a file. Therefore only move files that have no loaded markers.
     */

    // determine which failed files were loaded
    AccumuloClient client = manager.getContext();
    try (Scanner mscanner =
        new IsolatedScanner(client.createScanner(MetadataTable.NAME, Authorizations.EMPTY))) {
      mscanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());
      mscanner.fetchColumnFamily(BulkFileColumnFamily.NAME);

      for (Entry<Key,Value> entry : mscanner) {
        if (BulkFileColumnFamily.getBulkLoadTid(entry.getValue()) == tid) {
          Path loadedFile =
              new Path(ValidationUtil.validate(entry.getKey().getColumnQualifierData().toString()));
          if (failures.remove(loadedFile)) {
            loadedFailures.add(loadedFile);
          }
        }
      }
    }

    // move failed files that were not loaded
    for (Path orig : failures) {
      Path dest = new Path(error, orig.getName());
      fs.rename(orig, dest);
      log.debug(FateTxId.formatTid(tid) + " renamed " + orig + " to " + dest + ": import failed");
    }

    if (!loadedFailures.isEmpty()) {
      DistributedWorkQueue bifCopyQueue = new DistributedWorkQueue(
          Constants.ZROOT + "/" + manager.getInstanceID() + Constants.ZBULK_FAILED_COPYQ,
          manager.getConfiguration(), manager.getContext());

      HashSet<String> workIds = new HashSet<>();

      for (Path orig : loadedFailures) {
        Path dest = new Path(error, orig.getName());

        if (fs.exists(dest)) {
          continue;
        }

        bifCopyQueue.addWork(orig.getName(), (orig + "," + dest).getBytes(UTF_8));
        workIds.add(orig.getName());
        log.debug(
            FateTxId.formatTid(tid) + " added to copyq: " + orig + " to " + dest + ": failed");
      }

      bifCopyQueue.waitUntilDone(workIds);
    }

    fs.deleteRecursively(new Path(error, BulkImport.FAILURES_TXT));
    return new CleanUpBulkImport(tableId, source, bulk, error);
  }

}
