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
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.gc.Reference;
import org.apache.accumulo.core.gc.ReferenceDirectory;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.ValidationUtil;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * A single garbage collection performed on a table (Root, MD) or all User tables.
 */
public class GCRun implements GarbageCollectionEnvironment {
  private final Logger log;
  private final Ample.DataLevel level;
  private final ServerContext context;
  private final AccumuloConfiguration config;
  private long candidates = 0;
  private long inUse = 0;
  private long deleted = 0;
  private long errors = 0;

  public GCRun(Ample.DataLevel level, ServerContext context) {
    this.log = LoggerFactory.getLogger(level.name() + GCRun.class);
    this.level = level;
    this.context = context;
    this.config = context.getConfiguration();
  }

  @Override
  public Iterator<String> getCandidates() {
    return context.getAmple().getGcCandidates(level);
  }

  @Override
  public List<String> readCandidatesThatFitInMemory(Iterator<String> candidates) {
    long candidateLength = 0;
    // Converting the bytes to approximate number of characters for batch size.
    long candidateBatchSize = getCandidateBatchSize() / 2;

    List<String> candidatesBatch = new ArrayList<>();

    while (candidates.hasNext()) {
      String candidate = candidates.next();
      candidateLength += candidate.length();
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
      // combine all the entries read from file and scan columns in the metadata table
      var fileStream = Stream.concat(tm.getFiles().stream(), tm.getScans().stream());
      // map the files to Reference objects
      var stream = fileStream.map(f -> new ReferenceFile(tm.getTableId(), f.getMetaUpdateDelete()));
      // if dirName is populated then we have a tablet directory aka srv:dir
      if (tm.getDirName() != null) {
        // add the tablet directory to the stream
        var tabletDir = new ReferenceDirectory(tm.getTableId(), tm.getDirName());
        stream = Stream.concat(stream, Stream.of(tabletDir));
      }
      return stream;
    });

    var scanServerRefs = context.getAmple().getScanServerFileReferences()
        .map(sfr -> new ReferenceFile(sfr.getTableId(), sfr.getPathStr()));

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
  public void deleteConfirmedCandidates(SortedMap<String,String> confirmedDeletes)
      throws TableNotFoundException {
    final VolumeManager fs = context.getVolumeManager();
    var metadataLocation = level == Ample.DataLevel.ROOT
        ? context.getZooKeeperRoot() + " for " + RootTable.NAME : level.metaTable();

    if (inSafeMode()) {
      System.out.println("SAFEMODE: There are " + confirmedDeletes.size()
          + " data file candidates marked for deletion in " + metadataLocation + ".\n"
          + "          Examine the log files to identify them.\n");
      log.info("SAFEMODE: Listing all data file candidates for deletion");
      for (String s : confirmedDeletes.values()) {
        log.info("SAFEMODE: {}", s);
      }
      log.info("SAFEMODE: End candidates for deletion");
      return;
    }

    List<String> processedDeletes = Collections.synchronizedList(new ArrayList<>());

    minimizeDeletes(confirmedDeletes, processedDeletes, fs, log);

    ExecutorService deleteThreadPool = ThreadPools.getServerThreadPools()
        .createExecutorService(config, Property.GC_DELETE_THREADS, false);

    final List<Pair<Path,Path>> replacements = context.getVolumeReplacements();

    for (final String delete : confirmedDeletes.values()) {

      Runnable deleteTask = () -> {
        boolean removeFlag = false;

        try {
          Path fullPath;
          Path switchedDelete =
              VolumeUtil.switchVolume(delete, VolumeManager.FileType.TABLE, replacements);
          if (switchedDelete != null) {
            // actually replacing the volumes in the metadata table would be tricky because the
            // entries would be different rows. So it could not be
            // atomically in one mutation and extreme care would need to be taken that delete
            // entry was not lost. Instead of doing that, just deal with
            // volume switching when something needs to be deleted. Since the rest of the code
            // uses suffixes to compare delete entries, there is no danger
            // of deleting something that should not be deleted. Must not change value of delete
            // variable because that's what's stored in metadata table.
            log.debug("Volume replaced {} -> {}", delete, switchedDelete);
            fullPath = ValidationUtil.validate(switchedDelete);
          } else {
            fullPath = new Path(ValidationUtil.validate(delete));
          }

          for (Path pathToDel : GcVolumeUtil.expandAllVolumesUri(fs, fullPath)) {
            log.debug("Deleting {}", pathToDel);

            if (moveToTrash(pathToDel) || fs.deleteRecursively(pathToDel)) {
              // delete succeeded, still want to delete
              removeFlag = true;
              deleted++;
            } else if (fs.exists(pathToDel)) {
              // leave the entry in the metadata; we'll try again later
              removeFlag = false;
              errors++;
              log.warn("File exists, but was not deleted for an unknown reason: {}", pathToDel);
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
                    log.debug("File doesn't exist: {}", pathToDel);
                  }
                }
              } else {
                log.warn("Very strange path name: {}", delete);
              }
            }
          }

          // proceed to clearing out the flags for successful deletes and
          // non-existent files
          if (removeFlag) {
            processedDeletes.add(delete);
          }
        } catch (Exception e) {
          log.error("{}", e.getMessage(), e);
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

    context.getAmple().deleteGcCandidates(level, processedDeletes);
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
        log.debug("Removing table dir {}", p);
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

  @VisibleForTesting
  static void minimizeDeletes(SortedMap<String,String> confirmedDeletes,
      List<String> processedDeletes, VolumeManager fs, Logger logger) {
    Set<Path> seenVolumes = new HashSet<>();

    // when deleting a dir and all files in that dir, only need to delete the dir.
    // The dir will sort right before the files... so remove the files in this case
    // to minimize namenode ops
    Iterator<Map.Entry<String,String>> cdIter = confirmedDeletes.entrySet().iterator();

    String lastDirRel = null;
    Path lastDirAbs = null;
    while (cdIter.hasNext()) {
      Map.Entry<String,String> entry = cdIter.next();
      String relPath = entry.getKey();
      Path absPath = new Path(entry.getValue());

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
            logger.info("Ignoring {} because {} exist", entry.getValue(), lastDirAbs);
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
   * @return number of delete threads
   */
  boolean inSafeMode() {
    return context.getConfiguration().getBoolean(Property.GC_SAFEMODE);
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
      return false;
    }
    try {
      return fs.moveToTrash(path);
    } catch (FileNotFoundException ex) {
      return false;
    }
  }

  /**
   * Checks if the volume manager should move files to the trash rather than delete them.
   *
   * @return true if trash is used
   */
  boolean isUsingTrash() {
    return !config.getBoolean(Property.GC_TRASH_IGNORE);
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
