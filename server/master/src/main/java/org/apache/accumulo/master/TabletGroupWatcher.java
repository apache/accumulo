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
package org.apache.accumulo.master;

import static java.lang.Math.min;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.master.Master.TabletGoalState;
import org.apache.accumulo.master.state.MergeStats;
import org.apache.accumulo.master.state.TableCounts;
import org.apache.accumulo.master.state.TableStats;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.ClosableIterator;
import org.apache.accumulo.server.master.state.DistributedStoreException;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.master.state.TabletStateStore;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;

import com.google.common.collect.Iterators;

class TabletGroupWatcher extends Daemon {
  // Constants used to make sure assignment logging isn't excessive in quantity or size
  private static final String ASSIGNMENT_BUFFER_SEPARATOR = ", ";
  private static final int ASSINGMENT_BUFFER_MAX_LENGTH = 4096;

  private final Master master;
  final TabletStateStore store;
  final TabletGroupWatcher dependentWatcher;

  final TableStats stats = new TableStats();

  TabletGroupWatcher(Master master, TabletStateStore store, TabletGroupWatcher dependentWatcher) {
    this.master = master;
    this.store = store;
    this.dependentWatcher = dependentWatcher;
  }

  Map<Text,TableCounts> getStats() {
    return stats.getLast();
  }

  TableCounts getStats(Text tableId) {
    return stats.getLast(tableId);
  }

  @Override
  public void run() {

    Thread.currentThread().setName("Watching " + store.name());
    int[] oldCounts = new int[TabletState.values().length];
    EventCoordinator.Listener eventListener = this.master.nextEvent.getListener();

    while (this.master.stillMaster()) {
      // slow things down a little, otherwise we spam the logs when there are many wake-up events
      UtilWaitThread.sleep(100);

      int totalUnloaded = 0;
      int unloaded = 0;
      ClosableIterator<TabletLocationState> iter = null;
      try {
        Map<Text,MergeStats> mergeStatsCache = new HashMap<Text,MergeStats>();
        Map<Text,MergeStats> currentMerges = new HashMap<Text,MergeStats>();
        for (MergeInfo merge : master.merges()) {
          if (merge.getExtent() != null) {
            currentMerges.put(merge.getExtent().getTableId(), new MergeStats(merge));
          }
        }

        // Get the current status for the current list of tservers
        SortedMap<TServerInstance,TabletServerStatus> currentTServers = new TreeMap<TServerInstance,TabletServerStatus>();
        for (TServerInstance entry : this.master.tserverSet.getCurrentServers()) {
          currentTServers.put(entry, this.master.tserverStatus.get(entry));
        }

        if (currentTServers.size() == 0) {
          eventListener.waitForEvents(Master.TIME_TO_WAIT_BETWEEN_SCANS);
          continue;
        }

        // Don't move tablets to servers that are shutting down
        SortedMap<TServerInstance,TabletServerStatus> destinations = new TreeMap<TServerInstance,TabletServerStatus>(currentTServers);
        destinations.keySet().removeAll(this.master.serversToShutdown);

        List<Assignment> assignments = new ArrayList<Assignment>();
        List<Assignment> assigned = new ArrayList<Assignment>();
        List<TabletLocationState> assignedToDeadServers = new ArrayList<TabletLocationState>();
        Map<KeyExtent,TServerInstance> unassigned = new HashMap<KeyExtent,TServerInstance>();

        int[] counts = new int[TabletState.values().length];
        stats.begin();
        // Walk through the tablets in our store, and work tablets
        // towards their goal
        iter = store.iterator();
        while (iter.hasNext()) {
          TabletLocationState tls = iter.next();
          if (tls == null) {
            continue;
          }
          // ignore entries for tables that do not exist in zookeeper
          if (TableManager.getInstance().getTableState(tls.extent.getTableId().toString()) == null)
            continue;

          if (Master.log.isTraceEnabled())
            Master.log.trace(tls + " walogs " + tls.walogs.size());

          // Don't overwhelm the tablet servers with work
          if (unassigned.size() + unloaded > Master.MAX_TSERVER_WORK_CHUNK * currentTServers.size()) {
            flushChanges(destinations, assignments, assigned, assignedToDeadServers, unassigned);
            assignments.clear();
            assigned.clear();
            assignedToDeadServers.clear();
            unassigned.clear();
            unloaded = 0;
            eventListener.waitForEvents(Master.TIME_TO_WAIT_BETWEEN_SCANS);
          }
          Text tableId = tls.extent.getTableId();
          MergeStats mergeStats = mergeStatsCache.get(tableId);
          if (mergeStats == null) {
            mergeStats = currentMerges.get(tableId);
            if (mergeStats == null) {
              mergeStats = new MergeStats(new MergeInfo());
            }
            mergeStatsCache.put(tableId, mergeStats);
          }
          TabletGoalState goal = this.master.getGoalState(tls, mergeStats.getMergeInfo());
          TServerInstance server = tls.getServer();
          TabletState state = tls.getState(currentTServers.keySet());
          if (Master.log.isTraceEnabled())
            Master.log.trace("Goal state " + goal + " current " + state);
          stats.update(tableId, state);
          mergeStats.update(tls.extent, state, tls.chopped, !tls.walogs.isEmpty());
          sendChopRequest(mergeStats.getMergeInfo(), state, tls);
          sendSplitRequest(mergeStats.getMergeInfo(), state, tls);

          // Always follow through with assignments
          if (state == TabletState.ASSIGNED) {
            goal = TabletGoalState.HOSTED;
          }

          // if we are shutting down all the tabletservers, we have to do it in order
          if (goal == TabletGoalState.UNASSIGNED && state == TabletState.HOSTED) {
            if (this.master.serversToShutdown.equals(currentTServers.keySet())) {
              if (dependentWatcher != null && dependentWatcher.assignedOrHosted() > 0) {
                goal = TabletGoalState.HOSTED;
              }
            }
          }

          if (goal == TabletGoalState.HOSTED) {
            if (state != TabletState.HOSTED && !tls.walogs.isEmpty()) {
              if (this.master.recoveryManager.recoverLogs(tls.extent, tls.walogs))
                continue;
            }
            switch (state) {
              case HOSTED:
                if (server.equals(this.master.migrations.get(tls.extent)))
                  this.master.migrations.remove(tls.extent);
                break;
              case ASSIGNED_TO_DEAD_SERVER:
                assignedToDeadServers.add(tls);
                if (server.equals(this.master.migrations.get(tls.extent)))
                  this.master.migrations.remove(tls.extent);
                // log.info("Current servers " + currentTServers.keySet());
                break;
              case UNASSIGNED:
                // maybe it's a finishing migration
                TServerInstance dest = this.master.migrations.get(tls.extent);
                if (dest != null) {
                  // if destination is still good, assign it
                  if (destinations.keySet().contains(dest)) {
                    assignments.add(new Assignment(tls.extent, dest));
                  } else {
                    // get rid of this migration
                    this.master.migrations.remove(tls.extent);
                    unassigned.put(tls.extent, server);
                  }
                } else {
                  unassigned.put(tls.extent, server);
                }
                break;
              case ASSIGNED:
                // Send another reminder
                assigned.add(new Assignment(tls.extent, tls.future));
                break;
            }
          } else {
            switch (state) {
              case UNASSIGNED:
                break;
              case ASSIGNED_TO_DEAD_SERVER:
                assignedToDeadServers.add(tls);
                // log.info("Current servers " + currentTServers.keySet());
                break;
              case HOSTED:
                TServerConnection conn = this.master.tserverSet.getConnection(server);
                if (conn != null) {
                  conn.unloadTablet(this.master.masterLock, tls.extent, goal != TabletGoalState.DELETED);
                  unloaded++;
                  totalUnloaded++;
                } else {
                  Master.log.warn("Could not connect to server " + server);
                }
                break;
              case ASSIGNED:
                break;
            }
          }
          counts[state.ordinal()]++;
        }

        flushChanges(destinations, assignments, assigned, assignedToDeadServers, unassigned);

        // provide stats after flushing changes to avoid race conditions w/ delete table
        stats.end();

        // Report changes
        for (TabletState state : TabletState.values()) {
          int i = state.ordinal();
          if (counts[i] > 0 && counts[i] != oldCounts[i]) {
            this.master.nextEvent.event("[%s]: %d tablets are %s", store.name(), counts[i], state.name());
          }
        }
        Master.log.debug(String.format("[%s]: scan time %.2f seconds", store.name(), stats.getScanTime() / 1000.));
        oldCounts = counts;
        if (totalUnloaded > 0) {
          this.master.nextEvent.event("[%s]: %d tablets unloaded", store.name(), totalUnloaded);
        }

        updateMergeState(mergeStatsCache);

        Master.log.debug(String.format("[%s] sleeping for %.2f seconds", store.name(), Master.TIME_TO_WAIT_BETWEEN_SCANS / 1000.));
        eventListener.waitForEvents(Master.TIME_TO_WAIT_BETWEEN_SCANS);
      } catch (Exception ex) {
        Master.log.error("Error processing table state for store " + store.name(), ex);
        if (ex.getCause() != null && ex.getCause() instanceof BadLocationStateException) {
          repairMetadata(((BadLocationStateException) ex.getCause()).getEncodedEndRow());
        } else {
          UtilWaitThread.sleep(Master.WAIT_BETWEEN_ERRORS);
        }
      } finally {
        if (iter != null) {
          try {
            iter.close();
          } catch (IOException ex) {
            Master.log.warn("Error closing TabletLocationState iterator: " + ex, ex);
          }
        }
      }
    }
  }

  private void repairMetadata(Text row) {
    Master.log.debug("Attempting repair on " + row);
    // ACCUMULO-2261 if a dying tserver writes a location before its lock information propagates, it may cause duplicate assignment.
    // Attempt to find the dead server entry and remove it.
    try {
      Map<Key,Value> future = new HashMap<Key,Value>();
      Map<Key,Value> assigned = new HashMap<Key,Value>();
      KeyExtent extent = new KeyExtent(row, new Value(new byte[] {0}));
      String table = MetadataTable.NAME;
      if (extent.isMeta())
        table = RootTable.NAME;
      Scanner scanner = this.master.getConnector().createScanner(table, Authorizations.EMPTY);
      scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
      scanner.fetchColumnFamily(FutureLocationColumnFamily.NAME);
      scanner.setRange(new Range(row));
      for (Entry<Key,Value> entry : scanner) {
        if (entry.getKey().getColumnFamily().equals(CurrentLocationColumnFamily.NAME)) {
          assigned.put(entry.getKey(), entry.getValue());
        } else if (entry.getKey().getColumnFamily().equals(FutureLocationColumnFamily.NAME)) {
          future.put(entry.getKey(), entry.getValue());
        }
      }
      if (future.size() > 0 && assigned.size() > 0) {
        Master.log.warn("Found a tablet assigned and hosted, attempting to repair");
      } else if (future.size() > 1 && assigned.size() == 0) {
        Master.log.warn("Found a tablet assigned to multiple servers, attempting to repair");
      } else if (future.size() == 0 && assigned.size() > 1) {
        Master.log.warn("Found a tablet hosted on multiple servers, attempting to repair");
      } else {
        Master.log.info("Attempted a repair, but nothing seems to be obviously wrong. " + assigned + " " + future);
        return;
      }
      Iterator<Entry<Key,Value>> iter = Iterators.concat(future.entrySet().iterator(), assigned.entrySet().iterator());
      while (iter.hasNext()) {
        Entry<Key,Value> entry = iter.next();
        TServerInstance alive = master.tserverSet.find(entry.getValue().toString());
        if (alive == null) {
          Master.log.info("Removing entry " + entry);
          BatchWriter bw = this.master.getConnector().createBatchWriter(table, new BatchWriterConfig());
          Mutation m = new Mutation(entry.getKey().getRow());
          m.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
          bw.addMutation(m);
          bw.close();
          return;
        }
      }
      Master.log.error("Metadata table is inconsistent at " + row + " and all assigned/future tservers are still online.");
    } catch (Throwable e) {
      Master.log.error("Error attempting repair of metadata " + row + ": " + e, e);
    }
  }

  private int assignedOrHosted() {
    int result = 0;
    for (TableCounts counts : stats.getLast().values()) {
      result += counts.assigned() + counts.hosted();
    }
    return result;
  }

  private void sendSplitRequest(MergeInfo info, TabletState state, TabletLocationState tls) {
    // Already split?
    if (!info.getState().equals(MergeState.SPLITTING))
      return;
    // Merges don't split
    if (!info.isDelete())
      return;
    // Online and ready to split?
    if (!state.equals(TabletState.HOSTED))
      return;
    // Does this extent cover the end points of the delete?
    KeyExtent range = info.getExtent();
    if (tls.extent.overlaps(range)) {
      for (Text splitPoint : new Text[] {range.getPrevEndRow(), range.getEndRow()}) {
        if (splitPoint == null)
          continue;
        if (!tls.extent.contains(splitPoint))
          continue;
        if (splitPoint.equals(tls.extent.getEndRow()))
          continue;
        if (splitPoint.equals(tls.extent.getPrevEndRow()))
          continue;
        try {
          TServerConnection conn;
          conn = this.master.tserverSet.getConnection(tls.current);
          if (conn != null) {
            Master.log.info("Asking " + tls.current + " to split " + tls.extent + " at " + splitPoint);
            conn.splitTablet(this.master.masterLock, tls.extent, splitPoint);
          } else {
            Master.log.warn("Not connected to server " + tls.current);
          }
        } catch (NotServingTabletException e) {
          Master.log.debug("Error asking tablet server to split a tablet: " + e);
        } catch (Exception e) {
          Master.log.warn("Error asking tablet server to split a tablet: " + e);
        }
      }
    }
  }

  private void sendChopRequest(MergeInfo info, TabletState state, TabletLocationState tls) {
    // Don't bother if we're in the wrong state
    if (!info.getState().equals(MergeState.WAITING_FOR_CHOPPED))
      return;
    // Tablet must be online
    if (!state.equals(TabletState.HOSTED))
      return;
    // Tablet isn't already chopped
    if (tls.chopped)
      return;
    // Tablet ranges intersect
    if (info.needsToBeChopped(tls.extent)) {
      TServerConnection conn;
      try {
        conn = this.master.tserverSet.getConnection(tls.current);
        if (conn != null) {
          Master.log.info("Asking " + tls.current + " to chop " + tls.extent);
          conn.chop(this.master.masterLock, tls.extent);
        } else {
          Master.log.warn("Could not connect to server " + tls.current);
        }
      } catch (TException e) {
        Master.log.warn("Communications error asking tablet server to chop a tablet");
      }
    }
  }

  private void updateMergeState(Map<Text,MergeStats> mergeStatsCache) {
    for (MergeStats stats : mergeStatsCache.values()) {
      try {
        MergeState update = stats.nextMergeState(this.master.getConnector(), this.master);
        // when next state is MERGING, its important to persist this before
        // starting the merge... the verification check that is done before
        // moving into the merging state could fail if merge starts but does
        // not finish
        if (update == MergeState.COMPLETE)
          update = MergeState.NONE;
        if (update != stats.getMergeInfo().getState()) {
          this.master.setMergeState(stats.getMergeInfo(), update);
        }

        if (update == MergeState.MERGING) {
          try {
            if (stats.getMergeInfo().isDelete()) {
              deleteTablets(stats.getMergeInfo());
            } else {
              mergeMetadataRecords(stats.getMergeInfo());
            }
            this.master.setMergeState(stats.getMergeInfo(), update = MergeState.COMPLETE);
          } catch (Exception ex) {
            Master.log.error("Unable merge metadata table records", ex);
          }
        }
      } catch (Exception ex) {
        Master.log.error("Unable to update merge state for merge " + stats.getMergeInfo().getExtent(), ex);
      }
    }
  }

  private void deleteTablets(MergeInfo info) throws AccumuloException {
    KeyExtent extent = info.getExtent();
    String targetSystemTable = extent.isMeta() ? RootTable.NAME : MetadataTable.NAME;
    Master.log.debug("Deleting tablets for " + extent);
    char timeType = '\0';
    KeyExtent followingTablet = null;
    if (extent.getEndRow() != null) {
      Key nextExtent = new Key(extent.getEndRow()).followingKey(PartialKey.ROW);
      followingTablet = getHighTablet(new KeyExtent(extent.getTableId(), nextExtent.getRow(), extent.getEndRow()));
      Master.log.debug("Found following tablet " + followingTablet);
    }
    try {
      Connector conn = this.master.getConnector();
      Text start = extent.getPrevEndRow();
      if (start == null) {
        start = new Text();
      }
      Master.log.debug("Making file deletion entries for " + extent);
      Range deleteRange = new Range(KeyExtent.getMetadataEntry(extent.getTableId(), start), false, KeyExtent.getMetadataEntry(extent.getTableId(),
          extent.getEndRow()), true);
      Scanner scanner = conn.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(deleteRange);
      TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
      TabletsSection.ServerColumnFamily.TIME_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      scanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
      Set<FileRef> datafiles = new TreeSet<FileRef>();
      for (Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        if (key.compareColumnFamily(DataFileColumnFamily.NAME) == 0) {
          datafiles.add(new FileRef(this.master.fs, key));
          if (datafiles.size() > 1000) {
            MetadataTableUtil.addDeleteEntries(extent, datafiles, SystemCredentials.get());
            datafiles.clear();
          }
        } else if (TabletsSection.ServerColumnFamily.TIME_COLUMN.hasColumns(key)) {
          timeType = entry.getValue().toString().charAt(0);
        } else if (key.compareColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME) == 0) {
          throw new IllegalStateException("Tablet " + key.getRow() + " is assigned during a merge!");
        } else if (TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(key)) {
          // ACCUMULO-2974 Need to include the TableID when converting a relative path to an absolute path.
          // The value has the leading path separator already included so it doesn't need it included.
          String path = entry.getValue().toString();
          if (path.contains(":")) {
            datafiles.add(new FileRef(path));
          } else {
            datafiles.add(new FileRef(path, this.master.fs.getFullPath(FileType.TABLE, Path.SEPARATOR + extent.getTableId() + path)));
          }
          if (datafiles.size() > 1000) {
            MetadataTableUtil.addDeleteEntries(extent, datafiles, SystemCredentials.get());
            datafiles.clear();
          }
        }
      }
      MetadataTableUtil.addDeleteEntries(extent, datafiles, SystemCredentials.get());
      BatchWriter bw = conn.createBatchWriter(targetSystemTable, new BatchWriterConfig());
      try {
        deleteTablets(info, deleteRange, bw, conn);
      } finally {
        bw.close();
      }

      if (followingTablet != null) {
        Master.log.debug("Updating prevRow of " + followingTablet + " to " + extent.getPrevEndRow());
        bw = conn.createBatchWriter(targetSystemTable, new BatchWriterConfig());
        try {
          Mutation m = new Mutation(followingTablet.getMetadataEntry());
          TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(m, KeyExtent.encodePrevEndRow(extent.getPrevEndRow()));
          ChoppedColumnFamily.CHOPPED_COLUMN.putDelete(m);
          bw.addMutation(m);
          bw.flush();
        } finally {
          bw.close();
        }
      } else {
        // Recreate the default tablet to hold the end of the table
        Master.log.debug("Recreating the last tablet to point to " + extent.getPrevEndRow());
        String tdir = master.getFileSystem().choose(ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + extent.getTableId()
            + Constants.DEFAULT_TABLET_LOCATION;
        MetadataTableUtil.addTablet(new KeyExtent(extent.getTableId(), null, extent.getPrevEndRow()), tdir, SystemCredentials.get(), timeType,
            this.master.masterLock);
      }
    } catch (Exception ex) {
      throw new AccumuloException(ex);
    }
  }

  private void mergeMetadataRecords(MergeInfo info) throws AccumuloException {
    KeyExtent range = info.getExtent();
    Master.log.debug("Merging metadata for " + range);
    KeyExtent stop = getHighTablet(range);
    Master.log.debug("Highest tablet is " + stop);
    Value firstPrevRowValue = null;
    Text stopRow = stop.getMetadataEntry();
    Text start = range.getPrevEndRow();
    if (start == null) {
      start = new Text();
    }
    Range scanRange = new Range(KeyExtent.getMetadataEntry(range.getTableId(), start), false, stopRow, false);
    String targetSystemTable = MetadataTable.NAME;
    if (range.isMeta()) {
      targetSystemTable = RootTable.NAME;
    }

    BatchWriter bw = null;
    try {
      long fileCount = 0;
      Connector conn = this.master.getConnector();
      // Make file entries in highest tablet
      bw = conn.createBatchWriter(targetSystemTable, new BatchWriterConfig());
      Scanner scanner = conn.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(scanRange);
      TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      TabletsSection.ServerColumnFamily.TIME_COLUMN.fetch(scanner);
      TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      Mutation m = new Mutation(stopRow);
      String maxLogicalTime = null;
      for (Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        Value value = entry.getValue();
        if (key.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
          m.put(key.getColumnFamily(), key.getColumnQualifier(), value);
          fileCount++;
        } else if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key) && firstPrevRowValue == null) {
          Master.log.debug("prevRow entry for lowest tablet is " + value);
          firstPrevRowValue = new Value(value);
        } else if (TabletsSection.ServerColumnFamily.TIME_COLUMN.hasColumns(key)) {
          maxLogicalTime = TabletTime.maxMetadataTime(maxLogicalTime, value.toString());
        } else if (TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(key)) {
          bw.addMutation(MetadataTableUtil.createDeleteMutation(range.getTableId().toString(), entry.getValue().toString()));
        }
      }

      // read the logical time from the last tablet in the merge range, it is not included in
      // the loop above
      scanner = conn.createScanner(targetSystemTable, Authorizations.EMPTY);
      scanner.setRange(new Range(stopRow));
      TabletsSection.ServerColumnFamily.TIME_COLUMN.fetch(scanner);
      for (Entry<Key,Value> entry : scanner) {
        if (TabletsSection.ServerColumnFamily.TIME_COLUMN.hasColumns(entry.getKey())) {
          maxLogicalTime = TabletTime.maxMetadataTime(maxLogicalTime, entry.getValue().toString());
        }
      }

      if (maxLogicalTime != null)
        TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m, new Value(maxLogicalTime.getBytes()));

      if (!m.getUpdates().isEmpty()) {
        bw.addMutation(m);
      }

      bw.flush();

      Master.log.debug("Moved " + fileCount + " files to " + stop);

      if (firstPrevRowValue == null) {
        Master.log.debug("tablet already merged");
        return;
      }

      stop.setPrevEndRow(KeyExtent.decodePrevEndRow(firstPrevRowValue));
      Mutation updatePrevRow = stop.getPrevRowUpdateMutation();
      Master.log.debug("Setting the prevRow for last tablet: " + stop);
      bw.addMutation(updatePrevRow);
      bw.flush();

      deleteTablets(info, scanRange, bw, conn);

      // Clean-up the last chopped marker
      m = new Mutation(stopRow);
      ChoppedColumnFamily.CHOPPED_COLUMN.putDelete(m);
      bw.addMutation(m);
      bw.flush();

    } catch (Exception ex) {
      throw new AccumuloException(ex);
    } finally {
      if (bw != null)
        try {
          bw.close();
        } catch (Exception ex) {
          throw new AccumuloException(ex);
        }
    }
  }

  private void deleteTablets(MergeInfo info, Range scanRange, BatchWriter bw, Connector conn) throws TableNotFoundException, MutationsRejectedException {
    Scanner scanner;
    Mutation m;
    // Delete everything in the other tablets
    // group all deletes into tablet into one mutation, this makes tablets
    // either disappear entirely or not all.. this is important for the case
    // where the process terminates in the loop below...
    scanner = conn.createScanner(info.getExtent().isMeta() ? RootTable.NAME : MetadataTable.NAME, Authorizations.EMPTY);
    Master.log.debug("Deleting range " + scanRange);
    scanner.setRange(scanRange);
    RowIterator rowIter = new RowIterator(scanner);
    while (rowIter.hasNext()) {
      Iterator<Entry<Key,Value>> row = rowIter.next();
      m = null;
      while (row.hasNext()) {
        Entry<Key,Value> entry = row.next();
        Key key = entry.getKey();

        if (m == null)
          m = new Mutation(key.getRow());

        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        Master.log.debug("deleting entry " + key);
      }
      bw.addMutation(m);
    }

    bw.flush();
  }

  private KeyExtent getHighTablet(KeyExtent range) throws AccumuloException {
    try {
      Connector conn = this.master.getConnector();
      Scanner scanner = conn.createScanner(range.isMeta() ? RootTable.NAME : MetadataTable.NAME, Authorizations.EMPTY);
      TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      KeyExtent start = new KeyExtent(range.getTableId(), range.getEndRow(), null);
      scanner.setRange(new Range(start.getMetadataEntry(), null));
      Iterator<Entry<Key,Value>> iterator = scanner.iterator();
      if (!iterator.hasNext()) {
        throw new AccumuloException("No last tablet for a merge " + range);
      }
      Entry<Key,Value> entry = iterator.next();
      KeyExtent highTablet = new KeyExtent(entry.getKey().getRow(), KeyExtent.decodePrevEndRow(entry.getValue()));
      if (highTablet.getTableId() != range.getTableId()) {
        throw new AccumuloException("No last tablet for merge " + range + " " + highTablet);
      }
      return highTablet;
    } catch (Exception ex) {
      throw new AccumuloException("Unexpected failure finding the last tablet for a merge " + range, ex);
    }
  }

  private void flushChanges(SortedMap<TServerInstance,TabletServerStatus> currentTServers, List<Assignment> assignments, List<Assignment> assigned,
      List<TabletLocationState> assignedToDeadServers, Map<KeyExtent,TServerInstance> unassigned) throws DistributedStoreException, TException {
    if (!assignedToDeadServers.isEmpty()) {
      int maxServersToShow = min(assignedToDeadServers.size(), 100);
      Master.log.debug(assignedToDeadServers.size() + " assigned to dead servers: " + assignedToDeadServers.subList(0, maxServersToShow) + "...");
      store.unassign(assignedToDeadServers);
      this.master.nextEvent.event("Marked %d tablets as unassigned because they don't have current servers", assignedToDeadServers.size());
    }

    if (!currentTServers.isEmpty()) {
      Map<KeyExtent,TServerInstance> assignedOut = new HashMap<KeyExtent,TServerInstance>();
      final StringBuilder builder = new StringBuilder(64);
      this.master.tabletBalancer.getAssignments(Collections.unmodifiableSortedMap(currentTServers), Collections.unmodifiableMap(unassigned), assignedOut);
      for (Entry<KeyExtent,TServerInstance> assignment : assignedOut.entrySet()) {
        if (unassigned.containsKey(assignment.getKey())) {
          if (assignment.getValue() != null) {
            if (!currentTServers.containsKey(assignment.getValue())) {
              Master.log.warn("balancer assigned " + assignment.getKey() + " to a tablet server that is not current " + assignment.getValue() + " ignoring");
              continue;
            }

            if (builder.length() > 0) {
              builder.append(ASSIGNMENT_BUFFER_SEPARATOR);
            }

            builder.append(assignment);

            // Don't let the log message get too gigantic
            if (builder.length() > ASSINGMENT_BUFFER_MAX_LENGTH) {
              builder.append("]");
              Master.log.debug(store.name() + " assigning tablets: [" + builder.toString());
              builder.setLength(0);
            }

            assignments.add(new Assignment(assignment.getKey(), assignment.getValue()));
          }
        } else {
          Master.log.warn(store.name() + " load balancer assigning tablet that was not nominated for assignment " + assignment.getKey());
        }
      }

      if (builder.length() > 0) {
        // Make sure to log any leftover assignments
        builder.append("]");
        Master.log.debug(store.name() + " assigning tablets: [" + builder.toString());
      }

      if (!unassigned.isEmpty() && assignedOut.isEmpty())
        Master.log.warn("Load balancer failed to assign any tablets");
    }

    if (assignments.size() > 0) {
      Master.log.info(String.format("Assigning %d tablets", assignments.size()));
      store.setFutureLocations(assignments);
    }
    assignments.addAll(assigned);
    for (Assignment a : assignments) {
      TServerConnection conn = this.master.tserverSet.getConnection(a.server);
      if (conn != null) {
        conn.assignTablet(this.master.masterLock, a.tablet);
      } else {
        Master.log.warn("Could not connect to server " + a.server);
      }
      master.assignedTablet(a.tablet);
    }
  }

}
