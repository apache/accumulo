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
package org.apache.accumulo.master.state;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.master.state.CurrentState;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeStats {
  final static private Logger log = LoggerFactory.getLogger(MergeStats.class);
  MergeInfo info;
  int hosted = 0;
  int unassigned = 0;
  int chopped = 0;
  int needsToBeChopped = 0;
  int total = 0;
  boolean lowerSplit = false;
  boolean upperSplit = false;

  public MergeStats(MergeInfo info) {
    this.info = info;
    if (info.getState().equals(MergeState.NONE))
      return;
    if (info.getExtent().getEndRow() == null)
      upperSplit = true;
    if (info.getExtent().getPrevEndRow() == null)
      lowerSplit = true;
  }

  public MergeInfo getMergeInfo() {
    return info;
  }

  public void update(KeyExtent ke, TabletState state, boolean chopped, boolean hasWALs) {
    if (info.getState().equals(MergeState.NONE))
      return;
    if (!upperSplit && info.getExtent().getEndRow().equals(ke.getPrevEndRow())) {
      log.info("Upper split found");
      upperSplit = true;
    }
    if (!lowerSplit && info.getExtent().getPrevEndRow().equals(ke.getEndRow())) {
      log.info("Lower split found");
      lowerSplit = true;
    }
    if (!info.overlaps(ke))
      return;
    if (info.needsToBeChopped(ke)) {
      this.needsToBeChopped++;
      if (chopped) {
        if (state.equals(TabletState.HOSTED)) {
          this.chopped++;
        } else if (!hasWALs) {
          this.chopped++;
        }
      }
    }
    this.total++;
    if (state.equals(TabletState.HOSTED))
      this.hosted++;
    if (state.equals(TabletState.UNASSIGNED) || state.equals(TabletState.SUSPENDED))
      this.unassigned++;
  }

  public MergeState nextMergeState(Connector connector, CurrentState master) throws Exception {
    MergeState state = info.getState();
    if (state == MergeState.NONE)
      return state;
    if (total == 0) {
      log.trace("failed to see any tablets for this range, ignoring {}", info.getExtent());
      return state;
    }
    log.info("Computing next merge state for {} which is presently {} isDelete : {}", info.getExtent(), state, info.isDelete());
    if (state == MergeState.STARTED) {
      state = MergeState.SPLITTING;
    }
    if (state == MergeState.SPLITTING) {
      log.info("{} are hosted, total {}", hosted, total);
      if (!info.isDelete() && total == 1) {
        log.info("Merge range is already contained in a single tablet {}", info.getExtent());
        state = MergeState.COMPLETE;
      } else if (hosted == total) {
        if (info.isDelete()) {
          if (!lowerSplit)
            log.info("Waiting for {} lower split to occur {}", info, info.getExtent());
          else if (!upperSplit)
            log.info("Waiting for {} upper split to occur {}", info, info.getExtent());
          else
            state = MergeState.WAITING_FOR_CHOPPED;
        } else {
          state = MergeState.WAITING_FOR_CHOPPED;
        }
      } else {
        log.info("Waiting for {} hosted tablets to be {} {}", hosted, total, info.getExtent());
      }
    }
    if (state == MergeState.WAITING_FOR_CHOPPED) {
      log.info("{} tablets are chopped {}", chopped, info.getExtent());
      if (chopped == needsToBeChopped) {
        state = MergeState.WAITING_FOR_OFFLINE;
      } else {
        log.info("Waiting for {} chopped tablets to be {} {}", chopped, needsToBeChopped, info.getExtent());
      }
    }
    if (state == MergeState.WAITING_FOR_OFFLINE) {
      if (chopped != needsToBeChopped) {
        log.warn("Unexpected state: chopped tablets should be {} was {} merge {}", needsToBeChopped, chopped, info.getExtent());
        // Perhaps a split occurred after we chopped, but before we went offline: start over
        state = MergeState.WAITING_FOR_CHOPPED;
      } else {
        log.info("{} tablets are chopped, {} are offline {}", chopped, unassigned, info.getExtent());
        if (unassigned == total && chopped == needsToBeChopped) {
          if (verifyMergeConsistency(connector, master))
            state = MergeState.MERGING;
          else
            log.info("Merge consistency check failed {}", info.getExtent());
        } else {
          log.info("Waiting for {} unassigned tablets to be {} {}", unassigned, total, info.getExtent());
        }
      }
    }
    if (state == MergeState.MERGING) {
      if (hosted != 0) {
        // Shouldn't happen
        log.error("Unexpected state: hosted tablets should be zero {} merge {}", hosted, info.getExtent());
        state = MergeState.WAITING_FOR_OFFLINE;
      }
      if (unassigned != total) {
        // Shouldn't happen
        log.error("Unexpected state: unassigned tablets should be {} was {} merge {}", total, unassigned, info.getExtent());
        state = MergeState.WAITING_FOR_CHOPPED;
      }
      log.info("{} tablets are unassigned {}", unassigned, info.getExtent());
    }
    return state;
  }

  private boolean verifyMergeConsistency(Connector connector, CurrentState master) throws TableNotFoundException, IOException {
    MergeStats verify = new MergeStats(info);
    KeyExtent extent = info.getExtent();
    Scanner scanner = connector.createScanner(extent.isMeta() ? RootTable.NAME : MetadataTable.NAME, Authorizations.EMPTY);
    MetaDataTableScanner.configureScanner(scanner, master);
    Text start = extent.getPrevEndRow();
    if (start == null) {
      start = new Text();
    }
    Table.ID tableId = extent.getTableId();
    Text first = KeyExtent.getMetadataEntry(tableId, start);
    Range range = new Range(first, false, null, true);
    scanner.setRange(range.clip(MetadataSchema.TabletsSection.getRange()));
    KeyExtent prevExtent = null;

    log.debug("Scanning range {}", range);
    for (Entry<Key,Value> entry : scanner) {
      TabletLocationState tls;
      try {
        tls = MetaDataTableScanner.createTabletLocationState(entry.getKey(), entry.getValue());
      } catch (BadLocationStateException e) {
        log.error("{}", e.getMessage(), e);
        return false;
      }
      log.debug("consistency check: {} walogs {}", tls, tls.walogs.size());
      if (!tls.extent.getTableId().equals(tableId)) {
        break;
      }

      if (!tls.walogs.isEmpty() && verify.getMergeInfo().needsToBeChopped(tls.extent)) {
        log.debug("failing consistency: needs to be chopped {}", tls.extent);
        return false;
      }

      if (prevExtent == null) {
        // this is the first tablet observed, it must be offline and its prev row must be less than the start of the merge range
        if (tls.extent.getPrevEndRow() != null && tls.extent.getPrevEndRow().compareTo(start) > 0) {
          log.debug("failing consistency: prev row is too high {}", start);
          return false;
        }

        if (tls.getState(master.onlineTabletServers()) != TabletState.UNASSIGNED && tls.getState(master.onlineTabletServers()) != TabletState.SUSPENDED) {
          log.debug("failing consistency: assigned or hosted {}", tls);
          return false;
        }

      } else if (!tls.extent.isPreviousExtent(prevExtent)) {
        log.debug("hole in {}", MetadataTable.NAME);
        return false;
      }

      prevExtent = tls.extent;

      verify.update(tls.extent, tls.getState(master.onlineTabletServers()), tls.chopped, !tls.walogs.isEmpty());
      // stop when we've seen the tablet just beyond our range
      if (tls.extent.getPrevEndRow() != null && extent.getEndRow() != null && tls.extent.getPrevEndRow().compareTo(extent.getEndRow()) > 0) {
        break;
      }
    }
    log.debug("chopped {} v.chopped {} unassigned {} v.unassigned {} verify.total {}", chopped, verify.chopped, unassigned, verify.unassigned, verify.total);

    return chopped == verify.chopped && unassigned == verify.unassigned && unassigned == verify.total;
  }

  public static void main(String[] args) throws Exception {
    ClientOpts opts = new ClientOpts();
    opts.parseArgs(MergeStats.class.getName(), args);

    Connector conn = opts.getConnector();
    Map<String,String> tableIdMap = conn.tableOperations().tableIdMap();
    for (Entry<String,String> entry : tableIdMap.entrySet()) {
      final String table = entry.getKey(), tableId = entry.getValue();
      String path = ZooUtil.getRoot(conn.getInstance().getInstanceID()) + Constants.ZTABLES + "/" + tableId + "/merge";
      MergeInfo info = new MergeInfo();
      if (ZooReaderWriter.getInstance().exists(path)) {
        byte[] data = ZooReaderWriter.getInstance().getData(path, new Stat());
        DataInputBuffer in = new DataInputBuffer();
        in.reset(data, data.length);
        info.readFields(in);
      }
      System.out.println(String.format("%25s  %10s %10s %s", table, info.getState(), info.getOperation(), info.getExtent()));
    }
  }
}
