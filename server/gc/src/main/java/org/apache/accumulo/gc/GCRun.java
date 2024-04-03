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
package org.apache.accumulo.gc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.DIR;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SCANS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.gc.Reference;
import org.apache.accumulo.core.gc.ReferenceDirectory;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.ValidationUtil;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.GcCandidateType;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.gc.GcVolumeUtil;
import org.apache.accumulo.server.replication.proto.Replication;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A single garbage collection performed on a table (Root, MD) or all User tables.
 */
public class GCRun implements GarbageCollectionEnvironment {
  // loggers are not static to support unique naming by level
  private final Logger log;
  private static final String fileActionPrefix = "FILE-ACTION:";
  private final Ample.DataLevel level;
  private final ServerContext context;
  private final AccumuloConfiguration config;
  private long candidates = 0;
  private long inUse = 0;
  private long deleted = 0;
  private long errors = 0;

  public GCRun(Ample.DataLevel level, ServerContext context) {
    this.log = LoggerFactory.getLogger(GCRun.class.getName() + "." + level.name());
    this.level = level;
    this.context = context;
    this.config = context.getConfiguration();
  }

  @Override
  public Iterator<GcCandidate> getCandidates() {
    return context.getAmple().getGcCandidates(level);
  }

  /**
   * Removes gcCandidates from the metadata location depending on type.
   *
   * @param gcCandidates Collection of deletion reference candidates to remove.
   * @param type type of deletion reference candidates.
   */
  @Override
  public void deleteGcCandidates(Collection<GcCandidate> gcCandidates, GcCandidateType type) {
    if (inSafeMode()) {
      System.out.println("SAFEMODE: There are " + gcCandidates.size()
          + " reference file gcCandidates entries marked for deletion from " + level + " of type: "
          + type + ".\n          Examine the log files to identify them.\n");
      log.info("SAFEMODE: Listing all ref file gcCandidates for deletion");
      for (GcCandidate gcCandidate : gcCandidates) {
        log.info("SAFEMODE: {}", gcCandidate);
      }
      log.info("SAFEMODE: End reference candidates for deletion");
      return;
    }

    log.info("Attempting to delete gcCandidates of type {} from metadata", type);
    context.getAmple().deleteGcCandidates(level, gcCandidates, type);
  }

  @Override
  public List<GcCandidate> readCandidatesThatFitInMemory(Iterator<GcCandidate> candidates) {
    long candidateLength = 0;
    // Converting the bytes to approximate number of characters for batch size.
    long candidateBatchSize = getCandidateBatchSize() / 2;

    List<GcCandidate> candidatesBatch = new ArrayList<>();

    while (candidates.hasNext()) {
      GcCandidate candidate = candidates.next();
      candidateLength += candidate.getPath().length();
      candidatesBatch.add(candidate);
      if (candidateLength > candidateBatchSize) {
        log.info("Candidate batch of size {} has exceeded the threshold. Attempting to delete "
            + "what has been gathered so far.", candidateLength);
        return candidatesBatch;
      }
    }
    return candidatesBatch;
  }

  @Override
  public Stream<String> getBlipPaths() throws TableNotFoundException {

    if (level == Ample.DataLevel.ROOT) {
      return Stream.empty();
    }

    int blipPrefixLen = MetadataSchema.BlipSection.getRowPrefix().length();
    var scanner =
        new IsolatedScanner(context.createScanner(level.metaTable(), Authorizations.EMPTY));
    scanner.setRange(MetadataSchema.BlipSection.getRange());
    return scanner.stream()
        .map(entry -> entry.getKey().getRow().toString().substring(blipPrefixLen))
        .onClose(scanner::close);
  }

  @Override
  public Stream<Reference> getReferences() {
    Stream<TabletMetadata> tabletStream;

    // create a stream of metadata entries read from file, scan and tablet dir columns
    if (level == Ample.DataLevel.ROOT) {
      tabletStream = Stream.of(context.getAmple().readTablet(RootTable.EXTENT, DIR, FILES, SCANS));
    } else {
      var tabletsMetadata = TabletsMetadata.builder(context).scanTable(level.metaTable())
          .checkConsistency().fetch(DIR, FILES, SCANS).build();
      tabletStream = tabletsMetadata.stream();
    }

    // there is a lot going on in this "one line" so see below for more info
    var tabletReferences = tabletStream.flatMap(tm -> {
      var tableId = tm.getTableId();

      // verify that dir and prev row entries present for to check for complete row scan
      log.trace("tablet metadata table id: {}, end row:{}, dir:{}, saw: {}, prev row: {}", tableId,
          tm.getEndRow(), tm.getDirName(), tm.sawPrevEndRow(), tm.getPrevEndRow());
      if (tm.getDirName() == null || tm.getDirName().isEmpty() || !tm.sawPrevEndRow()) {
        throw new IllegalStateException("possible incomplete metadata scan for table id: " + tableId
            + ", end row: " + tm.getEndRow() + ", dir: " + tm.getDirName() + ", saw prev row: "
            + tm.sawPrevEndRow());
      }

      // combine all the entries read from file and scan columns in the metadata table
      Stream<StoredTabletFile> stfStream = tm.getFiles().stream();
      // map the files to Reference objects
      var fileStream = stfStream.map(f -> ReferenceFile.forFile(tableId, f.getMetaUpdateDelete()));

      // scans are normally empty, so only introduce a layer of indirection when needed
      final var tmScans = tm.getScans();
      if (!tmScans.isEmpty()) {
        var scanStream =
            tmScans.stream().map(s -> ReferenceFile.forScan(tableId, s.getMetaUpdateDelete()));
        fileStream = Stream.concat(fileStream, scanStream);
      }
      // if dirName is populated, then we have a tablet directory aka srv:dir
      if (tm.getDirName() != null) {
        // add the tablet directory to the stream
        var tabletDir = new ReferenceDirectory(tableId, tm.getDirName());
        fileStream = Stream.concat(fileStream, Stream.of(tabletDir));
      }
      return fileStream;
    });

    var scanServerRefs = context.getAmple().getScanServerFileReferences()
        .map(sfr -> ReferenceFile.forScan(sfr.getTableId(), sfr.getPathStr()));

    return Stream.concat(tabletReferences, scanServerRefs);
  }

  @Override
  public Map<TableId,TableState> getTableIDs() throws InterruptedException {
    final String tablesPath = context.getZooKeeperRoot() + Constants.ZTABLES;
    final ZooReader zr = context.getZooReader();
    int retries = 1;
    IllegalStateException ioe = null;
    while (retries <= 10) {
      try {
        zr.sync(tablesPath);
        final Map<TableId,TableState> tids = new HashMap<>();
        for (String table : zr.getChildren(tablesPath)) {
          TableId tableId = TableId.of(table);
          TableState tableState = null;
          String statePath = context.getZooKeeperRoot() + Constants.ZTABLES + "/"
              + tableId.canonical() + Constants.ZTABLE_STATE;
          try {
            byte[] state = zr.getData(statePath);
            if (state == null) {
              tableState = TableState.UNKNOWN;
            } else {
              tableState = TableState.valueOf(new String(state, UTF_8));
            }
          } catch (NoNodeException e) {
            tableState = TableState.UNKNOWN;
          }
          tids.put(tableId, tableState);
        }
        return tids;
      } catch (KeeperException e) {
        retries++;
        if (ioe == null) {
          ioe = new IllegalStateException("Error getting table ids from ZooKeeper");
        }
        ioe.addSuppressed(e);
        log.error("Error getting tables from ZooKeeper, retrying in {} seconds", retries, e);
        UtilWaitThread.sleepUninterruptibly(retries, TimeUnit.SECONDS);
      }
    }
    throw ioe;
  }

  @Override
  public void deleteConfirmedCandidates(SortedMap<String,GcCandidate> confirmedDeletes)
      throws TableNotFoundException {
    final VolumeManager fs = context.getVolumeManager();
    var metadataLocation = level == Ample.DataLevel.ROOT
        ? context.getZooKeeperRoot() + " for " + RootTable.NAME : level.metaTable();

    if (inSafeMode()) {
      System.out.println("SAFEMODE: There are " + confirmedDeletes.size()
          + " data file candidates marked for deletion in " + metadataLocation + ".\n"
          + "          Examine the log files to identify them.\n");
      log.info("{} SAFEMODE: Listing all data file candidates for deletion", fileActionPrefix);
      for (GcCandidate candidate : confirmedDeletes.values()) {
        log.info("{} SAFEMODE: {}", fileActionPrefix, candidate);
      }
      log.info("SAFEMODE: End candidates for deletion");
      return;
    }

    List<GcCandidate> processedDeletes = Collections.synchronizedList(new ArrayList<>());

    minimizeDeletes(confirmedDeletes, processedDeletes, fs, log);

    ExecutorService deleteThreadPool = ThreadPools.getServerThreadPools()
        .createExecutorService(config, Property.GC_DELETE_THREADS);

    final List<Pair<Path,Path>> replacements = context.getVolumeReplacements();

    for (final GcCandidate delete : confirmedDeletes.values()) {

      Runnable deleteTask = () -> {
        boolean removeFlag = false;

        try {
          Path fullPath;
          Path switchedDelete =
              VolumeUtil.switchVolume(delete.getPath(), VolumeManager.FileType.TABLE, replacements);
          if (switchedDelete != null) {
            // actually replacing the volumes in the metadata table would be tricky because the
            // entries would be different rows. So it could not be
            // atomically in one mutation and extreme care would need to be taken that delete
            // entry was not lost. Instead of doing that, just deal with
            // volume switching when something needs to be deleted. Since the rest of the code
            // uses suffixes to compare delete entries, there is no danger
            // of deleting something that should not be deleted. Must not change value of delete
            // variable because that's what's stored in metadata table.
            log.debug("Volume replaced {} -> {}", delete.getPath(), switchedDelete);
            fullPath = ValidationUtil.validate(switchedDelete);
          } else {
            fullPath = new Path(ValidationUtil.validate(delete.getPath()));
          }

          for (Path pathToDel : GcVolumeUtil.expandAllVolumesUri(fs, fullPath)) {
            log.debug("{} Deleting {}", fileActionPrefix, pathToDel);

            if (moveToTrash(pathToDel) || fs.deleteRecursively(pathToDel)) {
              // delete succeeded, still want to delete
              removeFlag = true;
              deleted++;
            } else if (fs.exists(pathToDel)) {
              // leave the entry in the metadata; we'll try again later
              removeFlag = false;
              errors++;
              log.warn("{} File exists, but was not deleted for an unknown reason: {}",
                  fileActionPrefix, pathToDel);
              break;
            } else {
              // this failure, we still want to remove the metadata entry
              removeFlag = true;
              errors++;
              String[] parts = pathToDel.toString().split(Constants.ZTABLES)[1].split("/");
              if (parts.length > 2) {
                TableId tableId = TableId.of(parts[1]);
                String tabletDir = parts[2];
                context.getTableManager().updateTableStateCache(tableId);
                TableState tableState = context.getTableManager().getTableState(tableId);
                if (tableState != null && tableState != TableState.DELETING) {
                  // clone directories don't always exist
                  if (!tabletDir.startsWith(Constants.CLONE_PREFIX)) {
                    log.debug("{} File doesn't exist: {}", fileActionPrefix, pathToDel);
                  }
                }
              } else {
                log.warn("{} Delete failed due to invalid file path format: {}", fileActionPrefix,
                    delete.getPath());
              }
            }
          }

          // proceed to clearing out the flags for successful deletes and
          // non-existent files
          if (removeFlag) {
            processedDeletes.add(delete);
          }
        } catch (Exception e) {
          log.error("{} Exception while deleting files ", fileActionPrefix, e);
        }

      };

      deleteThreadPool.execute(deleteTask);
    }

    deleteThreadPool.shutdown();

    try {
      while (!deleteThreadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)) { // empty
      }
    } catch (InterruptedException e1) {
      log.error("{}", e1.getMessage(), e1);
    }

    deleteGcCandidates(processedDeletes, GcCandidateType.VALID);
  }

  @Override
  public void deleteTableDirIfEmpty(TableId tableID) throws IOException {
    final VolumeManager fs = context.getVolumeManager();
    // if dir exist and is empty, then empty list is returned...
    // hadoop 2.0 will throw an exception if the file does not exist
    for (String dir : context.getTablesDirs()) {
      FileStatus[] tabletDirs;
      try {
        tabletDirs = fs.listStatus(new Path(dir + "/" + tableID));
      } catch (FileNotFoundException ex) {
        continue;
      }

      if (tabletDirs.length == 0) {
        Path p = new Path(dir + "/" + tableID);
        log.debug("{} Removing table dir {}", fileActionPrefix, p);
        if (!moveToTrash(p)) {
          fs.delete(p);
        }
      }
    }
  }

  @Override
  public void incrementCandidatesStat(long i) {
    candidates += i;
  }

  @Override
  public void incrementInUseStat(long i) {
    inUse += i;
  }

  @Override
  @Deprecated
  public Iterator<Map.Entry<String,Replication.Status>> getReplicationNeededIterator() {
    AccumuloClient client = context;
    try {
      Scanner s = org.apache.accumulo.core.replication.ReplicationTable.getScanner(client);
      org.apache.accumulo.core.replication.ReplicationSchema.StatusSection.limit(s);
      return Iterators.transform(s.iterator(), input -> {
        String file = input.getKey().getRow().toString();
        Replication.Status stat;
        try {
          stat = Replication.Status.parseFrom(input.getValue().get());
        } catch (InvalidProtocolBufferException e) {
          log.warn("Could not deserialize protobuf for: {}", input.getKey());
          stat = null;
        }
        return Maps.immutableEntry(file, stat);
      });
    } catch (org.apache.accumulo.core.replication.ReplicationTableOfflineException e) {
      // No elements that we need to preclude
      return Collections.emptyIterator();
    }
  }

  @VisibleForTesting
  static void minimizeDeletes(SortedMap<String,GcCandidate> confirmedDeletes,
      List<GcCandidate> processedDeletes, VolumeManager fs, Logger logger) {
    Set<Path> seenVolumes = new HashSet<>();

    // when deleting a dir and all files in that dir, only need to delete the dir.
    // The dir will sort right before the files... so remove the files in this case
    // to minimize namenode ops
    Iterator<Map.Entry<String,GcCandidate>> cdIter = confirmedDeletes.entrySet().iterator();

    String lastDirRel = null;
    Path lastDirAbs = null;
    while (cdIter.hasNext()) {
      Map.Entry<String,GcCandidate> entry = cdIter.next();
      String relPath = entry.getKey();
      Path absPath = new Path(entry.getValue().getPath());

      if (SimpleGarbageCollector.isDir(relPath)) {
        lastDirRel = relPath;
        lastDirAbs = absPath;
      } else if (lastDirRel != null) {
        if (relPath.startsWith(lastDirRel)) {
          Path vol = VolumeManager.FileType.TABLE.getVolume(absPath);

          boolean sameVol = false;

          if (GcVolumeUtil.isAllVolumesUri(lastDirAbs)) {
            if (seenVolumes.contains(vol)) {
              sameVol = true;
            } else {
              for (Volume cvol : fs.getVolumes()) {
                if (cvol.containsPath(vol)) {
                  seenVolumes.add(vol);
                  sameVol = true;
                }
              }
            }
          } else {
            sameVol = Objects.equals(VolumeManager.FileType.TABLE.getVolume(lastDirAbs), vol);
          }

          if (sameVol) {
            logger.info("{} Ignoring {} because {} exist", fileActionPrefix,
                entry.getValue().getPath(), lastDirAbs);
            processedDeletes.add(entry.getValue());
            cdIter.remove();
          }
        } else {
          lastDirRel = null;
          lastDirAbs = null;
        }
      }
    }
  }

  /**
   * Checks if safemode is set - files will not be deleted.
   *
   * @return value of {@link Property#GC_SAFEMODE}
   */
  boolean inSafeMode() {
    return context.getConfiguration().getBoolean(Property.GC_SAFEMODE);
  }

  /**
   * Checks if InUse Candidates can be removed.
   *
   * @return value of {@link Property#GC_REMOVE_IN_USE_CANDIDATES}
   */
  @Override
  public boolean canRemoveInUseCandidates() {
    return context.getConfiguration().getBoolean(Property.GC_REMOVE_IN_USE_CANDIDATES);
  }

  /**
   * Moves a file to trash. If this garbage collector is not using trash, this method returns false
   * and leaves the file alone. If the file is missing, this method returns false as opposed to
   * throwing an exception.
   *
   * @return true if the file was moved to trash
   * @throws IOException if the volume manager encountered a problem
   */
  boolean moveToTrash(Path path) throws IOException {
    final VolumeManager fs = context.getVolumeManager();
    if (!isUsingTrash()) {
      log.trace("Accumulo Trash is disabled. Skipped for {}", path);
      return false;
    }
    try {
      boolean success = fs.moveToTrash(path);
      log.trace("Accumulo Trash enabled, moving to trash succeeded?: {}", success);
      return success;
    } catch (FileNotFoundException ex) {
      log.error("Error moving {} to trash", path, ex);
      return false;
    }
  }

  /**
   * Checks if the volume manager should move files to the trash rather than delete them.
   *
   * @return true if trash is used
   */
  boolean isUsingTrash() {
    @SuppressWarnings("removal")
    Property p = Property.GC_TRASH_IGNORE;
    return !config.getBoolean(p);
  }

  /**
   * Gets the batch size for garbage collecting.
   *
   * @return candidate batch size.
   */
  long getCandidateBatchSize() {
    return config.getAsBytes(Property.GC_CANDIDATE_BATCH_SIZE);
  }

  public long getInUseStat() {
    return inUse;
  }

  public long getDeletedStat() {
    return deleted;
  }

  public long getErrorsStat() {
    return errors;
  }

  public long getCandidatesStat() {
    return candidates;
  }

  /**
   * Return a set of all TableIDs in the
   * {@link org.apache.accumulo.core.metadata.schema.Ample.DataLevel} for which we are considering
   * deletes. When gathering candidates at DataLevel.USER this will return all user table ids in an
   * ONLINE or OFFLINE state. When gathering candidates at DataLevel.METADATA this will return the
   * table id for the accumulo.metadata table. When gathering candidates at DataLevel.ROOT this will
   * return the table id for the accumulo.root table.
   *
   * @return The table ids
   * @throws InterruptedException if interrupted when calling ZooKeeper
   */
  @Override
  public Set<TableId> getCandidateTableIDs() throws InterruptedException {
    if (level == DataLevel.ROOT) {
      return Set.of(RootTable.ID);
    } else if (level == DataLevel.METADATA) {
      return Set.of(MetadataTable.ID);
    } else if (level == DataLevel.USER) {
      Set<TableId> tableIds = new HashSet<>();
      getTableIDs().forEach((k, v) -> {
        if (v == TableState.ONLINE || v == TableState.OFFLINE) {
          // Don't return tables that are NEW, DELETING, or in an
          // UNKNOWN state.
          tableIds.add(k);
        }
      });
      tableIds.remove(MetadataTable.ID);
      tableIds.remove(RootTable.ID);
      return tableIds;
    } else {
      throw new IllegalArgumentException("Unexpected level in GC Env: " + this.level.name());
    }
  }
}
