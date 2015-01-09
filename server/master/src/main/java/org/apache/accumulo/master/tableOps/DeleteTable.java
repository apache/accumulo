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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

class CleanUp extends MasterRepo {

  final private static Logger log = Logger.getLogger(CleanUp.class);

  private static final long serialVersionUID = 1L;

  private String tableId, namespaceId;

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

  public CleanUp(String tableId, String namespaceId) {
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
    Range tableRange = new KeyExtent(new Text(tableId), null, null).toMetadataRange();
    Scanner scanner = master.getConnector().createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    MetaDataTableScanner.configureScanner(scanner, master);
    scanner.setRange(tableRange);

    KeyExtent prevExtent = null;
    for (Entry<Key,Value> entry : scanner) {
      TabletLocationState locationState = MetaDataTableScanner.createTabletLocationState(entry.getKey(), entry.getValue());
      if (!locationState.extent.isPreviousExtent(prevExtent)) {
        log.info("Still waiting for table to be deleted: " + tableId + " saw inconsistency " + prevExtent + " " + locationState.extent);
        done = false;
        break;
      }
      prevExtent = locationState.extent;

      TabletState state = locationState.getState(master.onlineTabletServers());
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
  public Repo<Master> call(long tid, Master master) throws Exception {

    master.clearMigrations(tableId);

    int refCount = 0;

    try {
      // look for other tables that references this table's files
      Connector conn = master.getConnector();
      BatchScanner bs = conn.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 8);
      try {
        Range allTables = MetadataSchema.TabletsSection.getRange();
        Range tableRange = MetadataSchema.TabletsSection.getRange(tableId);
        Range beforeTable = new Range(allTables.getStartKey(), true, tableRange.getStartKey(), false);
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
      } finally {
        bs.close();
      }

    } catch (Exception e) {
      refCount = -1;
      log.error("Failed to scan " + MetadataTable.NAME + " looking for references to deleted table " + tableId, e);
    }

    // remove metadata table entries
    try {
      // Intentionally do not pass master lock. If master loses lock, this operation may complete before master can kill itself.
      // If the master lock passed to deleteTable, it is possible that the delete mutations will be dropped. If the delete operations
      // are dropped and the operation completes, then the deletes will not be repeated.
      MetadataTableUtil.deleteTable(tableId, refCount != 0, SystemCredentials.get(), null);
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
      final AccumuloConfiguration conf = master.getConfiguration().getConfiguration();
      boolean archiveFiles = conf.getBoolean(Property.GC_FILE_ARCHIVE);

      // delete the map files
      try {
        VolumeManager fs = master.getFileSystem();
        for (String dir : ServerConstants.getTablesDirs()) {
          if (archiveFiles) {
            archiveFile(fs, dir, tableId);
          } else {
            fs.deleteRecursively(new Path(dir, tableId));
          }
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
      TableManager.getInstance().removeTable(tableId);
      Tables.clearCache(master.getInstance());
    } catch (Exception e) {
      log.error("Failed to find table id in zookeeper", e);
    }

    // remove any permissions associated with this table
    try {
      AuditedSecurityOperation.getInstance().deleteTable(SystemCredentials.get().toThrift(master.getInstance()), tableId, namespaceId);
    } catch (ThriftSecurityException e) {
      log.error(e.getMessage(), e);
    }

    Utils.unreserveTable(tableId, tid, true);
    Utils.unreserveNamespace(namespaceId, tid, false);

    Logger.getLogger(CleanUp.class).debug("Deleted table " + tableId);

    return null;
  }

  protected void archiveFile(VolumeManager fs, String dir, String tableId) throws IOException {
    Path tableDirectory = new Path(dir, tableId);
    Volume v = fs.getVolumeByPath(tableDirectory);
    String basePath = v.getBasePath();

    // Path component of URI
    String tableDirPath = tableDirectory.toUri().getPath();

    // Just the suffix of the path (after the Volume's base path)
    String tableDirSuffix = tableDirPath.substring(basePath.length());

    // Remove a leading path separator char because Path will treat the "child" as an absolute path with it
    if (Path.SEPARATOR_CHAR == tableDirSuffix.charAt(0)) {
      if (tableDirSuffix.length() > 1) {
        tableDirSuffix = tableDirSuffix.substring(1);
      } else {
        tableDirSuffix = "";
      }
    }

    // Get the file archive directory on this volume
    final Path fileArchiveDir = new Path(basePath, ServerConstants.FILE_ARCHIVE_DIR);

    // Make sure it exists just to be safe
    fs.mkdirs(fileArchiveDir);

    // The destination to archive this table to
    final Path destTableDir = new Path(fileArchiveDir, tableDirSuffix);

    log.debug("Archiving " + tableDirectory + " to " + tableDirectory);

    if (fs.exists(destTableDir)) {
      merge(fs, tableDirectory, destTableDir);
    } else {
      fs.rename(tableDirectory, destTableDir);
    }
  }

  protected void merge(VolumeManager fs, Path src, Path dest) throws IOException {
    for (FileStatus child : fs.listStatus(src)) {
      final String childName = child.getPath().getName();
      final Path childInSrc = new Path(src, childName), childInDest = new Path(dest, childName);

      @SuppressWarnings("deprecation")
      boolean isFile = fs.isFile(childInSrc), isDir = child.isDir();

      // TODO change to `child.isFile()` when Hadoop-1 support is dropped
      if (isFile) {
        if (fs.exists(childInDest)) {
          log.warn("File already exists in archive, ignoring. " + childInDest);
        } else {
          fs.rename(childInSrc, childInDest);
        }
        // TODO change to `child.isDirectory()` when Hadoop-1 support is dropped
      } else if (isDir) {
        if (fs.exists(childInDest)) {
          // Recurse
          merge(fs, childInSrc, childInDest);
        } else {
          fs.rename(childInSrc, childInDest);
        }
      } else {
        // Symlinks shouldn't exist in table directories..
        log.warn("Ignoring archiving of non file/directory: " + child);
      }
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    // nothing to do
  }

}

public class DeleteTable extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private String tableId, namespaceId;

  public DeleteTable(String tableId) {
    this.tableId = tableId;
    Instance inst = HdfsZooInstance.getInstance();
    this.namespaceId = Tables.getNamespaceId(inst, tableId);
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {

    return Utils.reserveNamespace(namespaceId, tid, false, false, TableOperation.DELETE) + Utils.reserveTable(tableId, tid, true, true, TableOperation.DELETE);
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    TableManager.getInstance().transitionTableState(tableId, TableState.DELETING);
    environment.getEventCoordinator().event("deleting table %s ", tableId);
    return new CleanUp(tableId, namespaceId);
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    Utils.unreserveNamespace(namespaceId, tid, false);
    Utils.unreserveTable(tableId, tid, true);
  }

}
