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
package org.apache.accumulo.manager.upgrade;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.RootTable.ZROOT_TABLET;
import static org.apache.accumulo.core.metadata.RootTable.ZROOT_TABLET_GC_CANDIDATES;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.server.util.MetadataTableUtil.EMPTY_TEXT;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection.SkewedKeyValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.util.ConfigPropertyUpgrader;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.gc.AllVolumesDirectory;
import org.apache.accumulo.server.gc.GcVolumeUtil;
import org.apache.accumulo.server.metadata.RootGcCandidates;
import org.apache.accumulo.server.metadata.TabletMutatorBase;
import org.apache.accumulo.server.util.PropUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Handles upgrading from 2.0 to 2.1.
 * <ul>
 * <li><strong>Rename master properties (Issue
 * <a href="https://github.com/apache/accumulo/issues/1640">#1640</a>):</strong> Rename any
 * ZooKeeper system properties that start with "master." to the equivalent property starting with
 * "manager." instead (see the {@code renameOldMasterPropsinZK(ServerContext)} method). Note that
 * this change was part of a larger effort to replace references to master with manager. See issues
 * <a href="https://github.com/apache/accumulo/issues/1641">#1641</a>,
 * <a href="https://github.com/apache/accumulo/issues/1642">#1642</a>, and
 * <a href="https://github.com/apache/accumulo/issues/1643">#1643</a> as well.</li>
 * </ul>
 */
public class Upgrader9to10 implements Upgrader {

  private static final Logger log = LoggerFactory.getLogger(Upgrader9to10.class);

  public static final String ZROOT_TABLET_LOCATION = ZROOT_TABLET + "/location";
  public static final String ZROOT_TABLET_FUTURE_LOCATION = ZROOT_TABLET + "/future_location";
  public static final String ZROOT_TABLET_LAST_LOCATION = ZROOT_TABLET + "/lastlocation";
  public static final String ZROOT_TABLET_WALOGS = ZROOT_TABLET + "/walogs";
  public static final String ZROOT_TABLET_CURRENT_LOGS = ZROOT_TABLET + "/current_logs";
  public static final String ZROOT_TABLET_PATH = ZROOT_TABLET + "/dir";
  public static final Value UPGRADED = SkewedKeyValue.NAME;
  public static final String OLD_DELETE_PREFIX = "~del";

  // effectively an 8MB batch size, since this number is the number of Chars
  public static final long CANDIDATE_BATCH_SIZE = 4_000_000;

  @Override
  public void upgradeZookeeper(ServerContext context) {
    validateACLs(context);
    upgradePropertyStorage(context);
    setMetaTableProps(context);
    upgradeRootTabletMetadata(context);
    createExternalCompactionNodes(context);
    // special case where old files need to be deleted
    dropSortedMapWALFiles(context);
    createScanServerNodes(context);
  }

  private void validateACLs(ServerContext context) {

    final AtomicBoolean aclErrorOccurred = new AtomicBoolean(false);
    final ZooReaderWriter zrw = context.getZooReaderWriter();
    final ZooKeeper zk = zrw.getZooKeeper();
    final String rootPath = context.getZooKeeperRoot();

    final Id zkDigest =
        ZooUtil.getZkDigestAuthId(context.getConfiguration().get(Property.INSTANCE_SECRET));
    final List<ACL> privateWithAuth = new ArrayList<>();
    privateWithAuth.add(new ACL(ZooDefs.Perms.ALL, zkDigest));
    final List<ACL> publicWithAuth = new ArrayList<>(privateWithAuth);
    publicWithAuth.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));

    try {
      ZKUtil.visitSubTreeDFS(zk, rootPath, false, (rc, path, ctx, name) -> {
        try {
          final Stat stat = new Stat();
          final List<ACL> acls = zk.getACL(path, stat);

          if (((path.equals(Constants.ZROOT) || path.equals(Constants.ZROOT + Constants.ZINSTANCES))
              && !acls.equals(ZooDefs.Ids.OPEN_ACL_UNSAFE))
              || (!privateWithAuth.equals(acls) && !publicWithAuth.equals(acls))) {
            log.error("ZNode at {} has unexpected ACL: {}", path, acls);
            aclErrorOccurred.set(true);
          } else {
            log.trace("ZNode at {} has expected ACL.", path);
          }
        } catch (KeeperException | InterruptedException e) {
          log.error("Error getting ACL for path: {}", path, e);
          aclErrorOccurred.set(true);
        }
      });
      if (aclErrorOccurred.get()) {
        throw new RuntimeException("Upgrade Failed! Error validating ZNode ACLs. "
            + "Check the log for specific failed paths, check ZooKeeper troubleshooting in user documentation "
            + "for instructions on how to fix.");
      }
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Upgrade Failed! Error validating nodes under " + rootPath, e);
    }
  }

  @Override
  public void upgradeRoot(ServerContext context) {
    upgradeRelativePaths(context, Ample.DataLevel.METADATA);
    upgradeDirColumns(context, Ample.DataLevel.METADATA);
    upgradeFileDeletes(context, Ample.DataLevel.METADATA);
  }

  @Override
  public void upgradeMetadata(ServerContext context) {
    upgradeRelativePaths(context, Ample.DataLevel.USER);
    upgradeDirColumns(context, Ample.DataLevel.USER);
    upgradeFileDeletes(context, Ample.DataLevel.USER);
  }

  /**
   * Convert system properties (if necessary) and all table properties to a single node
   */
  private void upgradePropertyStorage(ServerContext context) {
    log.info("Starting property conversion");
    ConfigPropertyUpgrader configUpgrader = new ConfigPropertyUpgrader();
    configUpgrader.doUpgrade(context.getInstanceID(), context.getZooReaderWriter());
    log.info("Completed property conversion");
  }

  /**
   * Setup properties for External compactions.
   */
  private void setMetaTableProps(ServerContext context) {
    try {
      // sets the compaction dispatcher props for the given table and service name
      BiConsumer<TableId,String> setDispatcherProps =
          (TableId tableId, String dispatcherService) -> {
            var dispatcherPropsMap = Map.of(Property.TABLE_COMPACTION_DISPATCHER.getKey(),
                SimpleCompactionDispatcher.class.getName(),
                Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", dispatcherService);
            PropUtil.setProperties(context, TablePropKey.of(context, tableId), dispatcherPropsMap);
          };

      // root compaction props
      setDispatcherProps.accept(RootTable.ID, "root");
      // metadata compaction props
      setDispatcherProps.accept(MetadataTable.ID, "meta");
    } catch (IllegalStateException ex) {
      throw new RuntimeException("Unable to set system table properties", ex);
    }
  }

  private void createScanServerNodes(ServerContext context) {
    final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    try {
      context.getZooReaderWriter().putPersistentData(
          context.getZooKeeperRoot() + Constants.ZSSERVERS, EMPTY_BYTE_ARRAY,
          NodeExistsPolicy.SKIP);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Unable to create scan server paths", e);
    }
  }

  private void createExternalCompactionNodes(ServerContext context) {

    final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    try {
      context.getZooReaderWriter().putPersistentData(
          context.getZooKeeperRoot() + Constants.ZCOORDINATOR, EMPTY_BYTE_ARRAY,
          NodeExistsPolicy.SKIP);
      context.getZooReaderWriter().putPersistentData(
          context.getZooKeeperRoot() + Constants.ZCOORDINATOR_LOCK, EMPTY_BYTE_ARRAY,
          NodeExistsPolicy.SKIP);
      context.getZooReaderWriter().putPersistentData(
          context.getZooKeeperRoot() + Constants.ZCOMPACTORS, EMPTY_BYTE_ARRAY,
          NodeExistsPolicy.SKIP);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Unable to create external compaction paths", e);
    }
  }

  /**
   * Improvements to the metadata and root tables were made in this version. See pull request
   * <a href="https://github.com/apache/accumulo/pull/1174">#1174</a> for more details.
   */
  private void upgradeRootTabletMetadata(ServerContext context) {
    String rootMetaSer = getFromZK(context, ZROOT_TABLET);

    if (rootMetaSer == null || rootMetaSer.isEmpty()) {
      String dir = getFromZK(context, ZROOT_TABLET_PATH);
      List<LogEntry> logs = getRootLogEntries(context);

      TServerInstance last = getLocation(context, ZROOT_TABLET_LAST_LOCATION);
      TServerInstance future = getLocation(context, ZROOT_TABLET_FUTURE_LOCATION);
      TServerInstance current = getLocation(context, ZROOT_TABLET_LOCATION);

      UpgradeMutator tabletMutator = new UpgradeMutator(context);

      tabletMutator.putPrevEndRow(RootTable.EXTENT.prevEndRow());

      tabletMutator.putDirName(upgradeDirColumn(dir));

      if (last != null) {
        tabletMutator.putLocation(last, LocationType.LAST);
      }

      if (future != null) {
        tabletMutator.putLocation(future, LocationType.FUTURE);
      }

      if (current != null) {
        tabletMutator.putLocation(current, LocationType.CURRENT);
      }

      logs.forEach(tabletMutator::putWal);

      Map<String,DataFileValue> files = cleanupRootTabletFiles(context.getVolumeManager(), dir);
      files.forEach((path, dfv) -> tabletMutator.putFile(new TabletFile(new Path(path)), dfv));

      tabletMutator.putTime(computeRootTabletTime(context, files.keySet()));

      tabletMutator.mutate();
    }

    try {
      context.getZooReaderWriter().putPersistentData(
          context.getZooKeeperRoot() + ZROOT_TABLET_GC_CANDIDATES,
          new RootGcCandidates().toJson().getBytes(UTF_8), NodeExistsPolicy.SKIP);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    // this operation must be idempotent, so deleting after updating is very important

    delete(context, ZROOT_TABLET_CURRENT_LOGS);
    delete(context, ZROOT_TABLET_FUTURE_LOCATION);
    delete(context, ZROOT_TABLET_LAST_LOCATION);
    delete(context, ZROOT_TABLET_LOCATION);
    delete(context, ZROOT_TABLET_WALOGS);
    delete(context, ZROOT_TABLET_PATH);
  }

  private static class UpgradeMutator extends TabletMutatorBase {

    private final ServerContext context;

    UpgradeMutator(ServerContext context) {
      super(context, RootTable.EXTENT);
      this.context = context;
    }

    @Override
    public void mutate() {
      Mutation mutation = getMutation();

      try {
        context.getZooReaderWriter().mutateOrCreate(
            context.getZooKeeperRoot() + RootTable.ZROOT_TABLET, new byte[0], currVal -> {
              // Earlier, it was checked that root tablet metadata did not exists. However the
              // earlier check does handle race conditions. Race conditions are unexpected. This is
              // a sanity check when making the update in ZK using compare and set. If this fails
              // and its not a bug, then its likely some concurrency issue. For example two managers
              // concurrently running upgrade could cause this to fail.
              Preconditions.checkState(currVal.length == 0,
                  "Expected root tablet metadata to be empty!");
              var rtm = new RootTabletMetadata();
              rtm.update(mutation);
              String json = rtm.toJson();
              log.info("Upgrading root tablet metadata, writing following to ZK : \n {}", json);
              return json.getBytes(UTF_8);
            });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }

  }

  protected TServerInstance getLocation(ServerContext context, String relpath) {
    String str = getFromZK(context, relpath);
    if (str == null) {
      return null;
    }

    String[] parts = str.split("[|]", 2);
    HostAndPort address = HostAndPort.fromString(parts[0]);
    if (parts.length > 1 && parts[1] != null && !parts[1].isEmpty()) {
      return new TServerInstance(address, parts[1]);
    } else {
      // a 1.2 location specification: DO NOT WANT
      return null;
    }
  }

  static List<LogEntry> getRootLogEntries(ServerContext context) {

    try {
      ArrayList<LogEntry> result = new ArrayList<>();

      ZooReaderWriter zoo = context.getZooReaderWriter();
      String root = context.getZooKeeperRoot() + ZROOT_TABLET_WALOGS;
      // there's a little race between getting the children and fetching
      // the data. The log can be removed in between.
      outer: while (true) {
        result.clear();
        for (String child : zoo.getChildren(root)) {
          try {
            @SuppressWarnings("removal")
            LogEntry e = LogEntry.fromBytes(zoo.getData(root + "/" + child));
            // upgrade from !0;!0<< -> +r<<
            e = new LogEntry(RootTable.EXTENT, 0, e.filename);
            result.add(e);
          } catch (KeeperException.NoNodeException ex) {
            // TODO I think this is a bug, probably meant to continue to while loop... was probably
            // a bug in the original code.
            continue outer;
          }
        }
        break;
      }

      return result;
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String getFromZK(ServerContext context, String relpath) {
    try {
      byte[] data = context.getZooReaderWriter().getData(context.getZooKeeperRoot() + relpath);
      if (data == null) {
        return null;
      }

      return new String(data, UTF_8);
    } catch (NoNodeException e) {
      return null;
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void delete(ServerContext context, String relpath) {
    try {
      context.getZooReaderWriter().recursiveDelete(context.getZooKeeperRoot() + relpath,
          NodeMissingPolicy.SKIP);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  MetadataTime computeRootTabletTime(ServerContext context, Collection<String> goodPaths) {

    try {
      long rtime = Long.MIN_VALUE;
      for (String good : goodPaths) {
        Path path = new Path(good);

        FileSystem ns = context.getVolumeManager().getFileSystemByPath(path);
        var tableConf = context.getTableConfiguration(RootTable.ID);
        long maxTime = -1;
        try (FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder()
            .forFile(path.toString(), ns, ns.getConf(), NoCryptoServiceFactory.NONE)
            .withTableConfiguration(tableConf).seekToBeginning().build()) {
          while (reader.hasTop()) {
            maxTime = Math.max(maxTime, reader.getTopKey().getTimestamp());
            reader.next();
          }
        }
        if (maxTime > rtime) {

          rtime = maxTime;
        }
      }

      if (rtime < 0) {
        throw new IllegalStateException("Unexpected root tablet logical time " + rtime);
      }

      return new MetadataTime(rtime, TimeType.LOGICAL);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static Map<String,DataFileValue> cleanupRootTabletFiles(VolumeManager fs, String dir) {

    try {
      FileStatus[] files = fs.listStatus(new Path(dir));

      Map<String,DataFileValue> goodFiles = new HashMap<>(files.length);

      for (FileStatus file : files) {

        String path = file.getPath().toString();
        if (file.getPath().toUri().getScheme() == null) {
          // depending on the behavior of HDFS, if list status does not return fully qualified
          // volumes
          // then could switch to the default volume
          throw new IllegalArgumentException("Require fully qualified paths " + file.getPath());
        }

        String filename = file.getPath().getName();

        // check for incomplete major compaction, this should only occur
        // for root tablet
        if (filename.startsWith("delete+")) {
          String expectedCompactedFile =
              path.substring(0, path.lastIndexOf("/delete+")) + "/" + filename.split("\\+")[1];
          if (fs.exists(new Path(expectedCompactedFile))) {
            // compaction finished, but did not finish deleting compacted files.. so delete it
            if (!fs.deleteRecursively(file.getPath())) {
              log.warn("Delete of file: {} return false", file.getPath());
            }
            continue;
          }
          // compaction did not finish, so put files back

          // reset path and filename for rest of loop
          filename = filename.split("\\+", 3)[2];
          path = path.substring(0, path.lastIndexOf("/delete+")) + "/" + filename;
          Path src = file.getPath();
          Path dst = new Path(path);

          if (!fs.rename(src, dst)) {
            throw new IOException("Rename " + src + " to " + dst + " returned false ");
          }
        }

        if (filename.endsWith("_tmp")) {
          log.warn("cleaning up old tmp file: {}", path);
          if (!fs.deleteRecursively(file.getPath())) {
            log.warn("Delete of tmp file: {} return false", file.getPath());
          }

          continue;
        }

        if (!filename.startsWith(Constants.MAPFILE_EXTENSION + "_")
            && !FileOperations.getValidExtensions().contains(filename.split("\\.")[1])) {
          log.error("unknown file in tablet: {}", path);
          continue;
        }

        goodFiles.put(path, new DataFileValue(file.getLen(), 0));
      }

      return goodFiles;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Improve how Delete markers are stored. For more information see:
   * <a href="https://github.com/apache/accumulo/issues/1043">#1043</a>
   * <a href="https://github.com/apache/accumulo/pull/1366">#1366</a>
   */
  public void upgradeFileDeletes(ServerContext context, Ample.DataLevel level) {

    String tableName = level.metaTable();
    Ample ample = context.getAmple();

    // find all deletes
    try (BatchWriter writer = context.createBatchWriter(tableName)) {
      log.info("looking for candidates in table {}", tableName);
      Iterator<String> oldCandidates = getOldCandidates(context, tableName);
      String upgradeProp =
          context.getConfiguration().get(Property.INSTANCE_VOLUMES_UPGRADE_RELATIVE);

      while (oldCandidates.hasNext()) {
        List<String> deletes = readCandidatesInBatch(oldCandidates);
        log.info("found {} deletes to upgrade", deletes.size());
        for (String olddelete : deletes) {
          // create new formatted delete
          log.trace("upgrading delete entry for {}", olddelete);

          Path absolutePath = resolveRelativeDelete(olddelete, upgradeProp);
          ReferenceFile updatedDel = switchToAllVolumes(absolutePath);

          writer.addMutation(ample.createDeleteMutation(updatedDel));
        }
        writer.flush();
        // if nothing thrown then we're good so mark all deleted
        log.info("upgrade processing completed so delete old entries");
        for (String olddelete : deletes) {
          log.trace("deleting old entry for {}", olddelete);
          writer.addMutation(deleteOldDeleteMutation(olddelete));
        }
        writer.flush();
      }
    } catch (TableNotFoundException | MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * If path of file to delete is a directory, change it to all volumes. See {@link GcVolumeUtil}.
   * For example: A directory "hdfs://localhost:9000/accumulo/tables/5a/t-0005" with volume removed
   * "tables/5a/t-0005" depth = 3 will be switched to "agcav:/tables/5a/t-0005". A file
   * "hdfs://localhost:9000/accumulo/tables/5a/t-0005/A0012.rf" with volume removed
   * "tables/5a/t-0005/A0012.rf" depth = 4 will be returned as is.
   */
  @VisibleForTesting
  static ReferenceFile switchToAllVolumes(Path olddelete) {
    Path pathNoVolume = Objects.requireNonNull(VolumeManager.FileType.TABLE.removeVolume(olddelete),
        "Invalid delete marker. No volume in path: " + olddelete);

    // a directory path with volume removed will have a depth of 3 like, "tables/5a/t-0005"
    if (pathNoVolume.depth() == 3) {
      String tabletDir = pathNoVolume.getName();
      var tableId = TableId.of(pathNoVolume.getParent().getName());
      // except bulk directories don't get an all volume prefix
      if (pathNoVolume.getName().startsWith(Constants.BULK_PREFIX)) {
        return new ReferenceFile(tableId, olddelete.toString());
      } else {
        return new AllVolumesDirectory(tableId, tabletDir);
      }
    } else {
      // depth of 4 should be a file like, "tables/5a/t-0005/A0012.rf"
      if (pathNoVolume.depth() == 4) {
        Path tabletDirPath = pathNoVolume.getParent();
        var tableId = TableId.of(tabletDirPath.getParent().getName());
        return new ReferenceFile(tableId, olddelete.toString());
      } else {
        throw new IllegalStateException("Invalid delete marker: " + olddelete);
      }
    }
  }

  /**
   * Return path of the file from old delete markers
   */
  private Iterator<String> getOldCandidates(ServerContext context, String tableName)
      throws TableNotFoundException {
    Range range = DeletesSection.getRange();
    Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY);
    scanner.setRange(range);
    return scanner.stream().filter(entry -> !entry.getValue().equals(UPGRADED))
        .map(entry -> entry.getKey().getRow().toString().substring(OLD_DELETE_PREFIX.length()))
        .iterator();
  }

  private List<String> readCandidatesInBatch(Iterator<String> candidates) {
    long candidateLength = 0;
    List<String> result = new ArrayList<>();
    while (candidates.hasNext()) {
      String candidate = candidates.next();
      candidateLength += candidate.length();
      result.add(candidate);
      if (candidateLength > CANDIDATE_BATCH_SIZE) {
        log.trace("List of delete candidates has exceeded the batch size"
            + " threshold. Attempting to delete what has been gathered so far.");
        break;
      }
    }
    return result;
  }

  private Mutation deleteOldDeleteMutation(final String delete) {
    Mutation m = new Mutation(OLD_DELETE_PREFIX + delete);
    m.putDelete(EMPTY_TEXT, EMPTY_TEXT);
    return m;
  }

  /**
   * Changes to how volumes were stored in the metadata and have Accumulo always call the volume
   * chooser for new tablet files. These changes were done in
   * <a href="https://github.com/apache/accumulo/pull/1389">#1389</a>
   */
  public void upgradeDirColumns(ServerContext context, Ample.DataLevel level) {
    String tableName = level.metaTable();

    try (Scanner scanner = context.createScanner(tableName, Authorizations.EMPTY);
        BatchWriter writer = context.createBatchWriter(tableName)) {
      DIRECTORY_COLUMN.fetch(scanner);

      for (Entry<Key,Value> entry : scanner) {
        Mutation m = new Mutation(entry.getKey().getRow());
        DIRECTORY_COLUMN.put(m, new Value(upgradeDirColumn(entry.getValue().toString())));
        writer.addMutation(m);
      }
    } catch (TableNotFoundException | AccumuloException e) {
      throw new RuntimeException(e);
    }
  }

  public static String upgradeDirColumn(String dir) {
    return new Path(dir).getName();
  }

  /**
   * Remove all file entries containing relative paths and replace them with absolute URI paths.
   * Absolute paths are resolved by prefixing relative paths with a volume configured by the user in
   * the instance.volumes.upgrade.relative property, which is only used during an upgrade. If any
   * relative paths are found and this property is not configured, or if any resolved absolute path
   * does not correspond to a file that actually exists, the upgrade step fails and aborts without
   * making changes. See the property {@link Property#INSTANCE_VOLUMES_UPGRADE_RELATIVE} and the
   * pull request <a href="https://github.com/apache/accumulo/pull/1461">#1461</a>.
   */
  public static void upgradeRelativePaths(ServerContext context, Ample.DataLevel level) {
    String tableName = level.metaTable();
    VolumeManager fs = context.getVolumeManager();
    String upgradeProp = context.getConfiguration().get(Property.INSTANCE_VOLUMES_UPGRADE_RELATIVE);

    // first pass check for relative paths - if any, check existence of the file path
    // constructed from the upgrade property + relative path
    if (checkForRelativePaths(context, fs, tableName, upgradeProp)) {
      log.info("Relative Tablet File paths exist in {}, replacing with absolute using {}",
          tableName, upgradeProp);
    } else {
      log.info("No relative paths found in {} during upgrade.", tableName);
      return;
    }

    // second pass, create atomic mutations to replace the relative path
    replaceRelativePaths(context, fs, tableName, upgradeProp);
  }

  /**
   * Replace relative paths but only if the constructed absolute path exists on FileSystem
   */
  public static void replaceRelativePaths(AccumuloClient c, VolumeManager fs, String tableName,
      String upgradeProperty) {
    try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY);
        BatchWriter writer = c.createBatchWriter(tableName)) {

      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      for (Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        String metaEntry = key.getColumnQualifier().toString();
        if (!metaEntry.contains(":")) {
          // found relative paths so get the property used to build the absolute paths
          if (upgradeProperty == null || upgradeProperty.isBlank()) {
            throw new IllegalArgumentException(
                "Missing required property " + Property.INSTANCE_VOLUMES_UPGRADE_RELATIVE.getKey());
          }
          Path relPath = resolveRelativePath(metaEntry, key);
          Path absPath = new Path(upgradeProperty, relPath);
          if (fs.exists(absPath)) {
            log.debug("Changing Tablet File path from {} to {}", metaEntry, absPath);
            Mutation m = new Mutation(key.getRow());
            // add the new path
            m.at().family(key.getColumnFamily()).qualifier(absPath.toString())
                .visibility(key.getColumnVisibility()).put(entry.getValue());
            // delete the old path
            m.at().family(key.getColumnFamily()).qualifier(key.getColumnQualifierData().toArray())
                .visibility(key.getColumnVisibility()).delete();
            writer.addMutation(m);
          } else {
            throw new IllegalArgumentException(
                "Relative Tablet file " + relPath + " not found at " + absPath);
          }
        }
      }
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new IllegalStateException(e);
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  /**
   * Check if table has any relative paths, return false if none are found. When a relative path is
   * found, check existence of the file path constructed from the upgrade property + relative path
   */
  public static boolean checkForRelativePaths(AccumuloClient client, VolumeManager fs,
      String tableName, String upgradeProperty) {
    boolean hasRelatives = false;

    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
      log.info("Looking for relative paths in {}", tableName);
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      for (Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        String metaEntry = key.getColumnQualifier().toString();
        if (!metaEntry.contains(":")) {
          // found relative paths so verify the property used to build the absolute paths
          hasRelatives = true;
          if (upgradeProperty == null || upgradeProperty.isBlank()) {
            throw new IllegalArgumentException(
                "Missing required property " + Property.INSTANCE_VOLUMES_UPGRADE_RELATIVE.getKey());
          }
          Path relPath = resolveRelativePath(metaEntry, key);
          Path absPath = new Path(upgradeProperty, relPath);
          if (!fs.exists(absPath)) {
            throw new IllegalArgumentException("Tablet file " + relPath + " not found at " + absPath
                + " using volume: " + upgradeProperty);
          }
        }
      }
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return hasRelatives;
  }

  /**
   * Resolve old-style relative paths, returning Path of everything except volume and base
   */
  private static Path resolveRelativePath(String metadataEntry, Key key) {
    String prefix = VolumeManager.FileType.TABLE.getDirectory() + "/";
    if (metadataEntry.startsWith("../")) {
      // resolve style "../2a/t-0003/C0004.rf"
      return new Path(prefix + metadataEntry.substring(3));
    } else {
      // resolve style "/t-0003/C0004.rf"
      TableId tableId = KeyExtent.fromMetaRow(key.getRow()).tableId();
      return new Path(prefix + tableId.canonical() + metadataEntry);
    }
  }

  /**
   * Resolve old relative delete markers of the form /tableId/tabletDir/[file] to
   * UpgradeVolume/tables/tableId/tabletDir/[file]
   */
  static Path resolveRelativeDelete(String oldDelete, String upgradeProperty) {
    Path pathNoVolume = VolumeManager.FileType.TABLE.removeVolume(new Path(oldDelete));
    Path pathToCheck = new Path(oldDelete);

    // if the volume was removed properly, the path is absolute so return, otherwise
    // it is a relative path so proceed with more checks
    if (pathNoVolume != null) {
      return pathToCheck;
    }

    // A relative path directory of the form "/tableId/tabletDir" will have depth == 2
    // A relative path file of the form "/tableId/tabletDir/file" will have depth == 3
    Preconditions.checkState(
        oldDelete.startsWith("/") && (pathToCheck.depth() == 2 || pathToCheck.depth() == 3),
        "Unrecognized relative delete marker {}", oldDelete);

    // found relative paths so verify the property used to build the absolute paths
    if (upgradeProperty == null || upgradeProperty.isBlank()) {
      throw new IllegalArgumentException(
          "Missing required property " + Property.INSTANCE_VOLUMES_UPGRADE_RELATIVE.getKey());
    }
    return new Path(upgradeProperty, VolumeManager.FileType.TABLE.getDirectory() + oldDelete);
  }

  /**
   * Remove old temporary map files to prevent problems during recovery. Sorted recovery was updated
   * to use RFiles instead of map files. So to prevent issues during tablet recovery, remove the old
   * temporary map files and resort using RFiles. For more information see the following issues:
   * <a href="https://github.com/apache/accumulo/issues/2117">#2117</a> and
   * <a href="https://github.com/apache/accumulo/issues/2179">#2179</a>
   */
  static void dropSortedMapWALFiles(ServerContext context) {
    VolumeManager vm = context.getVolumeManager();
    for (String recoveryDir : context.getRecoveryDirs()) {
      Path recoveryDirPath = new Path(recoveryDir);
      try {
        if (!vm.exists(recoveryDirPath)) {
          log.info("There are no recovery files in {}", recoveryDir);
          continue;
        }
        List<Path> directoriesToDrop = new ArrayList<>();
        for (FileStatus walDir : vm.listStatus(recoveryDirPath)) {
          // map files will be in a directory starting with "part"
          Path walDirPath = walDir.getPath();
          for (FileStatus dirOrFile : vm.listStatus(walDirPath)) {
            if (dirOrFile.isDirectory()) {
              directoriesToDrop.add(walDirPath);
              break;
            }
          }
        }
        if (!directoriesToDrop.isEmpty()) {
          log.info("Found {} old sorted map directories to delete.", directoriesToDrop.size());
          for (Path dir : directoriesToDrop) {
            log.info("Deleting everything in old sorted map directory: {}", dir);
            vm.deleteRecursively(dir);
          }
        }
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }
    }
  }
}
