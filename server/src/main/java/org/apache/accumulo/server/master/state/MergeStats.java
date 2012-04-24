/**
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
package org.apache.accumulo.server.master.state;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

public class MergeStats {
  final static private Logger log = Logger.getLogger(MergeStats.class);
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
    if (info.getRange().getEndRow() == null)
      upperSplit = true;
    if (info.getRange().getPrevEndRow() == null)
      lowerSplit = true;
  }
  
  public MergeInfo getMergeInfo() {
    return info;
  }

  public void update(KeyExtent ke, TabletState state, boolean chopped, boolean hasWALs) {
    if (info.getState().equals(MergeState.NONE))
      return;
    if (!upperSplit && info.getRange().getEndRow().equals(ke.getPrevEndRow())) {
      log.info("Upper split found");
      upperSplit = true;
    }
    if (!lowerSplit && info.getRange().getPrevEndRow().equals(ke.getEndRow())) {
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
    if (state.equals(TabletState.UNASSIGNED))
      this.unassigned++;
  }
  
  public MergeState nextMergeState(Connector connector, CurrentState master) throws Exception {
    MergeState state = info.getState();
    if (state == MergeState.NONE)
      return state;
    log.info("Computing next merge state for " + info.getRange() + " which is presently " + state + " isDelete : " + info.isDelete());
    if (state == MergeState.STARTED) {
      state = MergeState.SPLITTING;
    }
    if (total == 0) {
      log.info("failed to see any tablets for this range, ignoring " + info.getRange());
      return state;
    }
    if (total == 1 && info.getRange().isMeta()) {
      // root tablet watcher trying to merge metadata tablets it won't even scan
      log.debug("ignoring merge of " + info.getRange());
      return state;
    }
    if (state == MergeState.SPLITTING) {
      log.info(hosted + " are hosted, total " + total);
      if (!info.isDelete() && total == 1) {
        log.info("Merge range is already contained in a single tablet " + info.getRange());
        state = MergeState.COMPLETE;
      } else if (hosted == total) {
        if (info.isDelete()) {
          if (!lowerSplit)
            log.info("Waiting for " + info + " lower split to occur " + info.getRange());
          else if (!upperSplit)
            log.info("Waiting for " + info + " upper split to occur " + info.getRange());
          else
            state = MergeState.WAITING_FOR_CHOPPED;
        } else {
          state = MergeState.WAITING_FOR_CHOPPED;
        }
      } else {
        log.info("Waiting for " + hosted + " hosted tablets to be " + total + " " + info.getRange());
      }
    }
    if (state == MergeState.WAITING_FOR_CHOPPED) {
      log.info(chopped + " tablets are chopped " + info.getRange());
      if (chopped == needsToBeChopped) {
        state = MergeState.WAITING_FOR_OFFLINE;
      } else {
        log.info("Waiting for " + chopped + " chopped tablets to be " + needsToBeChopped + " " + info.getRange());
      }
    }
    if (state == MergeState.WAITING_FOR_OFFLINE) {
      if (chopped != needsToBeChopped) {
        log.warn("Unexpected state: chopped tablets should be " + needsToBeChopped + " was " + chopped + " merge " + info.getRange());
        // Perhaps a split occurred after we chopped, but before we went offline: start over
        state = MergeState.WAITING_FOR_CHOPPED;
      } else {
        log.info(chopped + " tablets are chopped, " + unassigned + " are offline " + info.getRange());
        if (unassigned == total && chopped == needsToBeChopped) {
          if (verifyMergeConsistency(connector, master))
            state = MergeState.MERGING;
          else
            log.info("Merge consistency check failed " + info.getRange());
        } else {
          log.info("Waiting for " + unassigned + " unassigned tablets to be " + total + " " + info.getRange());
        }
      }
    }
    if (state == MergeState.MERGING) {
      if (hosted != 0) {
        // Shouldn't happen
        log.error("Unexpected state: hosted tablets should be zero " + hosted + " merge " + info.getRange());
        state = MergeState.WAITING_FOR_OFFLINE;
      }
      if (unassigned != total) {
        // Shouldn't happen
        log.error("Unexpected state: unassigned tablets should be " + total + " was " + unassigned + " merge " + info.getRange());
        state = MergeState.WAITING_FOR_CHOPPED;
      }
      log.info(unassigned + " tablets are unassigned " + info.getRange());
    }
    return state;
  }
  
  private boolean verifyMergeConsistency(Connector connector, CurrentState master) throws TableNotFoundException, IOException {
    MergeStats verify = new MergeStats(info);
    Scanner scanner = connector.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
    MetaDataTableScanner.configureScanner(scanner, master);
    KeyExtent extent = info.getRange();
    Text start = extent.getPrevEndRow();
    if (start == null) {
      start = new Text();
    }
    Text tableId = extent.getTableId();
    Text first = KeyExtent.getMetadataEntry(tableId, start);
    Range range = new Range(first, false, null, true);
    if (extent.isMeta())
      range = new Range(Constants.METADATA_ROOT_TABLET_KEYSPACE);
    scanner.setRange(range);
    KeyExtent prevExtent = null;

    for (Entry<Key,Value> entry : scanner) {
      TabletLocationState tls = MetaDataTableScanner.createTabletLocationState(entry.getKey(), entry.getValue());
      if (!tls.extent.getTableId().equals(tableId)) {
        break;
      }

      if (!tls.walogs.isEmpty() && verify.getMergeInfo().needsToBeChopped(tls.extent)) {
        return false;
      }

      if (prevExtent == null) {
        // this is the first tablet observed, it must be offline and its prev row must be less than the start of the merge range
        if (tls.extent.getPrevEndRow() != null && tls.extent.getPrevEndRow().compareTo(start) > 0) {
          return false;
        }
        
        if (tls.getState(master.onlineTabletServers()) != TabletState.UNASSIGNED)
          return false;
        
      } else if (!tls.extent.isPreviousExtent(prevExtent)) {
        return false;
      }
      
      prevExtent = tls.extent;

      verify.update(tls.extent, tls.getState(master.onlineTabletServers()), tls.chopped, !tls.walogs.isEmpty());
      // stop when we've seen the tablet just beyond our range
      if (tls.extent.getPrevEndRow() != null && extent.getEndRow() != null && tls.extent.getPrevEndRow().compareTo(extent.getEndRow()) > 0) {
        break;
      }
    }
    return chopped == verify.chopped && unassigned == verify.unassigned && unassigned == verify.total;
  }
  
  public static void main(String[] args) throws Exception {
    Instance instance = HdfsZooInstance.getInstance();
    Map<String,String> tableIdMap = instance.getConnector(SecurityConstants.getSystemCredentials()).tableOperations().tableIdMap();
    for (String table : tableIdMap.keySet()) {
      String tableId = tableIdMap.get(table);
      String path = ZooUtil.getRoot(instance.getInstanceID()) + Constants.ZTABLES + "/" + tableId.toString() + "/merge";
      MergeInfo info = new MergeInfo();
      if (ZooReaderWriter.getInstance().exists(path)) {
        byte[] data = ZooReaderWriter.getInstance().getData(path, new Stat());
        DataInputBuffer in = new DataInputBuffer();
        in.reset(data, data.length);
        info.readFields(in);
      }
      System.out.println(String.format("%25s  %10s %10s %s", table, info.state, info.operation, info.range));
    }
  }
}