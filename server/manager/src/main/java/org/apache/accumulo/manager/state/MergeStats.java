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
package org.apache.accumulo.manager.state;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.manager.state.CurrentState;
import org.apache.accumulo.server.manager.state.MergeInfo;
import org.apache.accumulo.server.manager.state.MergeState;
import org.apache.accumulo.server.manager.state.MetaDataTableScanner;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class MergeStats {
  final static private Logger log = LoggerFactory.getLogger(MergeStats.class);
  private final MergeInfo info;
  private int hosted = 0;
  private int unassigned = 0;
  private int total = 0;

  public MergeStats(MergeInfo info) {
    this.info = info;
  }

  public MergeInfo getMergeInfo() {
    return info;
  }

  public void update(KeyExtent ke, TabletState state) {
    if (info.getState().equals(MergeState.NONE)) {
      return;
    }
    if (!info.overlaps(ke)) {
      return;
    }
    this.total++;
    if (state.equals(TabletState.HOSTED)) {
      this.hosted++;
    }
    if (state.equals(TabletState.UNASSIGNED) || state.equals(TabletState.SUSPENDED)) {
      this.unassigned++;
    }
  }

  public MergeState nextMergeState(AccumuloClient accumuloClient, CurrentState manager)
      throws Exception {
    MergeState state = info.getState();
    if (state == MergeState.NONE) {
      return state;
    }
    if (total == 0) {
      log.trace("failed to see any tablets for this range, ignoring {}", info.getExtent());
      return state;
    }
    log.info("Computing next merge state for {} which is presently {} isDelete : {}",
        info.getExtent(), state, info.isDelete());
    if (state == MergeState.STARTED) {
      log.info("{} are hosted, total {}", hosted, total);
      if (!info.isDelete() && total == 1) {
        log.info("Merge range is already contained in a single tablet {}", info.getExtent());
        state = MergeState.COMPLETE;
      } else if (hosted == total) {
        state = MergeState.WAITING_FOR_OFFLINE;
      } else {
        log.info("Waiting for {} hosted tablets to be {} {}", hosted, total, info.getExtent());
      }
    }
    if (state == MergeState.WAITING_FOR_OFFLINE) {
      if (unassigned == total) {
        if (verifyMergeConsistency(accumuloClient, manager)) {
          state = MergeState.MERGING;
        } else {
          log.info("Merge consistency check failed {}", info.getExtent());
        }
      } else {
        log.info("Waiting for {} unassigned tablets to be {} {}", unassigned, total,
            info.getExtent());
      }
    }
    if (state == MergeState.MERGING || state == MergeState.MERGED) {
      if (hosted != 0) {
        // Shouldn't happen
        log.error("Unexpected state: hosted tablets should be zero {} merge {}", hosted,
            info.getExtent());
        state = MergeState.WAITING_FOR_OFFLINE;
      }
      if (unassigned != total) {
        // Shouldn't happen
        log.error("Unexpected state: unassigned tablets should be {} was {} merge {}", total,
            unassigned, info.getExtent());
        state = MergeState.WAITING_FOR_OFFLINE;
      }
      log.info("{} tablets are unassigned {}", unassigned, info.getExtent());
    }
    return state;
  }

  private boolean verifyMergeConsistency(AccumuloClient accumuloClient, CurrentState manager)
      throws TableNotFoundException, IOException {
    // The only expected state when this method is called is WAITING_FOR_OFFLINE
    verifyState(info, MergeState.WAITING_FOR_OFFLINE);

    MergeStats verify = new MergeStats(info);
    KeyExtent extent = info.getExtent();
    Scanner scanner = accumuloClient.createScanner(
        extent.isMeta() ? AccumuloTable.ROOT.tableName() : AccumuloTable.METADATA.tableName(),
        Authorizations.EMPTY);
    MetaDataTableScanner.configureScanner(scanner, manager);
    Text start = extent.prevEndRow();
    if (start == null) {
      start = new Text();
    }
    TableId tableId = extent.tableId();
    Text first = TabletsSection.encodeRow(tableId, start);
    Range range = new Range(first, false, null, true);
    scanner.setRange(range.clip(TabletsSection.getRange()));
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
      if (!tls.extent.tableId().equals(tableId)) {
        break;
      }

      // Verify that no WALs exist
      if (!verifyWalogs(tls)) {
        log.debug("failing consistency: {} has walogs {}", tls.extent, tls.walogs.size());
        return false;
      }

      if (prevExtent == null) {
        // this is the first tablet observed, it must be offline and its prev row must be less than
        // the start of the merge range
        if (tls.extent.prevEndRow() != null && tls.extent.prevEndRow().compareTo(start) > 0) {
          log.debug("failing consistency: prev row is too high {}", start);
          return false;
        }

        if (tls.getState(manager.onlineTabletServers()) != TabletState.UNASSIGNED
            && tls.getState(manager.onlineTabletServers()) != TabletState.SUSPENDED) {
          log.debug("failing consistency: assigned or hosted {}", tls);
          return false;
        }

      } else if (!tls.extent.isPreviousExtent(prevExtent)) {
        log.debug("hole in {}", AccumuloTable.METADATA.tableName());
        return false;
      }

      prevExtent = tls.extent;

      verify.update(tls.extent, tls.getState(manager.onlineTabletServers()));
      // stop when we've seen the tablet just beyond our range
      if (tls.extent.prevEndRow() != null && extent.endRow() != null
          && tls.extent.prevEndRow().compareTo(extent.endRow()) > 0) {
        break;
      }
    }
    log.debug("unassigned {} v.unassigned {} verify.total {}", unassigned, verify.unassigned,
        verify.total);

    return unassigned == verify.unassigned && unassigned == verify.total;
  }

  @VisibleForTesting
  void verifyState(MergeInfo info, MergeState expectedState) {
    Preconditions.checkState(info.getState() == expectedState, "Unexpected merge state %s",
        info.getState());
  }

  @VisibleForTesting
  boolean verifyWalogs(TabletLocationState tls) {
    return tls.walogs.isEmpty();
  }

  public static void main(String[] args) throws Exception {
    ServerUtilOpts opts = new ServerUtilOpts();
    opts.parseArgs(MergeStats.class.getName(), args);

    Span span = TraceUtil.startSpan(MergeStats.class, "main");
    try (Scope scope = span.makeCurrent()) {
      try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
        Map<String,String> tableIdMap = client.tableOperations().tableIdMap();
        ZooReaderWriter zooReaderWriter = opts.getServerContext().getZooReaderWriter();
        for (Entry<String,String> entry : tableIdMap.entrySet()) {
          final String table = entry.getKey(), tableId = entry.getValue();
          String path = ZooUtil.getRoot(client.instanceOperations().getInstanceId())
              + Constants.ZTABLES + "/" + tableId + "/merge";
          MergeInfo info = new MergeInfo();
          if (zooReaderWriter.exists(path)) {
            byte[] data = zooReaderWriter.getData(path);
            DataInputBuffer in = new DataInputBuffer();
            in.reset(data, data.length);
            info.readFields(in);
          }
          System.out.printf("%25s  %10s %10s %s%n", table, info.getState(), info.getOperation(),
              info.getExtent());
        }
      }
    } finally {
      span.end();
    }
  }
}
