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
package org.apache.accumulo.master.tableOps;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CopyFailed extends MasterRepo {
  private static final Logger log = LoggerFactory.getLogger(CopyFailed.class);

  private static final long serialVersionUID = 1L;

  private Table.ID tableId;
  private String source;
  private String bulk;
  private String error;

  public CopyFailed(Table.ID tableId, String source, String bulk, String error) {
    this.tableId = tableId;
    this.source = source;
    this.bulk = bulk;
    this.error = error;
  }

  @Override
  public long isReady(long tid, Master master) throws Exception {
    Set<TServerInstance> finished = new HashSet<>();
    Set<TServerInstance> running = master.onlineTabletServers();
    for (TServerInstance server : running) {
      try {
        TServerConnection client = master.getConnection(server);
        if (client != null && !client.isActive(tid))
          finished.add(server);
      } catch (TException ex) {
        log.info("Ignoring error trying to check on tid " + tid + " from server " + server + ": " + ex);
      }
    }
    if (finished.containsAll(running))
      return 0;
    return 500;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    // This needs to execute after the arbiter is stopped
    master.updateBulkImportStatus(source, BulkImportState.COPY_FILES);
    VolumeManager fs = master.getFileSystem();

    if (!fs.exists(new Path(error, BulkImport.FAILURES_TXT)))
      return new CleanUpBulkImport(tableId, source, bulk, error);

    HashMap<FileRef,String> failures = new HashMap<>();
    HashMap<FileRef,String> loadedFailures = new HashMap<>();

    try (BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(error, BulkImport.FAILURES_TXT)), UTF_8))) {
      String line = null;
      while ((line = in.readLine()) != null) {
        Path path = new Path(line);
        if (!fs.exists(new Path(error, path.getName())))
          failures.put(new FileRef(line, path), line);
      }
    }

    /*
     * I thought I could move files that have no file references in the table. However its possible a clone references a file. Therefore only move files that
     * have no loaded markers.
     */

    // determine which failed files were loaded
    Connector conn = master.getConnector();
    try (Scanner mscanner = new IsolatedScanner(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY))) {
      mscanner.setRange(new KeyExtent(tableId, null, null).toMetadataRange());
      mscanner.fetchColumnFamily(TabletsSection.BulkFileColumnFamily.NAME);

      for (Entry<Key,Value> entry : mscanner) {
        if (Long.parseLong(entry.getValue().toString()) == tid) {
          FileRef loadedFile = new FileRef(fs, entry.getKey());
          String absPath = failures.remove(loadedFile);
          if (absPath != null) {
            loadedFailures.put(loadedFile, absPath);
          }
        }
      }
    }

    // move failed files that were not loaded
    for (String failure : failures.values()) {
      Path orig = new Path(failure);
      Path dest = new Path(error, orig.getName());
      fs.rename(orig, dest);
      log.debug("tid " + tid + " renamed " + orig + " to " + dest + ": import failed");
    }

    if (loadedFailures.size() > 0) {
      DistributedWorkQueue bifCopyQueue = new DistributedWorkQueue(Constants.ZROOT + "/" + master.getInstance().getInstanceID() + Constants.ZBULK_FAILED_COPYQ,
          master.getConfiguration());

      HashSet<String> workIds = new HashSet<>();

      for (String failure : loadedFailures.values()) {
        Path orig = new Path(failure);
        Path dest = new Path(error, orig.getName());

        if (fs.exists(dest))
          continue;

        bifCopyQueue.addWork(orig.getName(), (failure + "," + dest).getBytes(UTF_8));
        workIds.add(orig.getName());
        log.debug("tid " + tid + " added to copyq: " + orig + " to " + dest + ": failed");
      }

      bifCopyQueue.waitUntilDone(workIds);
    }

    fs.deleteRecursively(new Path(error, BulkImport.FAILURES_TXT));
    return new CleanUpBulkImport(tableId, source, bulk, error);
  }

}
