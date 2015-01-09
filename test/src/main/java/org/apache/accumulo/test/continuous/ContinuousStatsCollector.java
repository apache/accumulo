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
package org.apache.accumulo.test.continuous;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.ColumnFamilyCounter;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Stat;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.cli.ClientOnRequiredTable;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;

public class ContinuousStatsCollector {

  static class StatsCollectionTask extends TimerTask {

    private final String tableId;
    private final Opts opts;
    private final int scanBatchSize;

    public StatsCollectionTask(Opts opts, int scanBatchSize) {
      this.opts = opts;
      this.scanBatchSize = scanBatchSize;
      this.tableId = Tables.getNameToIdMap(opts.getInstance()).get(opts.tableName);
      System.out
          .println("TIME TABLET_SERVERS TOTAL_ENTRIES TOTAL_INGEST TOTAL_QUERY TABLE_RECS TABLE_RECS_IN_MEM TABLE_INGEST TABLE_QUERY TABLE_TABLETS TABLE_TABLETS_ONLINE"
              + " ACCUMULO_DU ACCUMULO_DIRS ACCUMULO_FILES TABLE_DU TABLE_DIRS TABLE_FILES"
              + " MAP_TASK MAX_MAP_TASK REDUCE_TASK MAX_REDUCE_TASK TASK_TRACKERS BLACK_LISTED MIN_FILES/TABLET MAX_FILES/TABLET AVG_FILES/TABLET STDDEV_FILES/TABLET");
    }

    @Override
    public void run() {
      try {
        String acuStats = getACUStats();
        String fsStats = getFSStats();
        String mrStats = getMRStats();
        String tabletStats = getTabletStats();

        System.out.println(System.currentTimeMillis() + " " + acuStats + " " + fsStats + " " + mrStats + " " + tabletStats);
      } catch (Exception e) {
        System.err.println(System.currentTimeMillis() + " Failed to collect stats : " + e.getMessage());
        e.printStackTrace();
      }
    }

    private String getTabletStats() throws Exception {

      Connector conn = opts.getConnector();
      Scanner scanner = conn.createScanner(MetadataTable.NAME, opts.auths);
      scanner.setBatchSize(scanBatchSize);
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      scanner.addScanIterator(new IteratorSetting(1000, "cfc", ColumnFamilyCounter.class.getName()));
      scanner.setRange(new KeyExtent(new Text(tableId), null, null).toMetadataRange());

      Stat s = new Stat();

      int count = 0;
      for (Entry<Key,Value> entry : scanner) {
        count++;
        s.addStat(Long.parseLong(entry.getValue().toString()));
      }

      if (count > 0)
        return String.format("%d %d %.3f %.3f", s.getMin(), s.getMax(), s.getAverage(), s.getStdDev());
      else
        return "0 0 0 0";

    }

    private String getFSStats() throws Exception {
      VolumeManager fs = VolumeManagerImpl.get();
      long length1 = 0, dcount1 = 0, fcount1 = 0;
      long length2 = 0, dcount2 = 0, fcount2 = 0;
      for (String dir : ServerConstants.getTablesDirs()) {
        ContentSummary contentSummary = fs.getContentSummary(new Path(dir));
        length1 += contentSummary.getLength();
        dcount1 += contentSummary.getDirectoryCount();
        fcount1 += contentSummary.getFileCount();
        contentSummary = fs.getContentSummary(new Path(dir, tableId));
        length2 += contentSummary.getLength();
        dcount2 += contentSummary.getDirectoryCount();
        fcount2 += contentSummary.getFileCount();
      }

      return "" + length1 + " " + dcount1 + " " + fcount1 + " " + length2 + " " + dcount2 + " " + fcount2;
    }

    private String getACUStats() throws Exception {

      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(opts.getInstance());
        MasterMonitorInfo stats = client.getMasterStats(Tracer.traceInfo(), SystemCredentials.get().toThrift(opts.getInstance()));

        TableInfo all = new TableInfo();
        Map<String,TableInfo> tableSummaries = new HashMap<String,TableInfo>();

        for (TabletServerStatus server : stats.tServerInfo) {
          for (Entry<String,TableInfo> info : server.tableMap.entrySet()) {
            TableInfo tableSummary = tableSummaries.get(info.getKey());
            if (tableSummary == null) {
              tableSummary = new TableInfo();
              tableSummaries.put(info.getKey(), tableSummary);
            }
            TableInfoUtil.add(tableSummary, info.getValue());
            TableInfoUtil.add(all, info.getValue());
          }
        }

        TableInfo ti = tableSummaries.get(tableId);

        return "" + stats.tServerInfo.size() + " " + all.recs + " " + (long) all.ingestRate + " " + (long) all.queryRate + " " + ti.recs + " "
            + ti.recsInMemory + " " + (long) ti.ingestRate + " " + (long) ti.queryRate + " " + ti.tablets + " " + ti.onlineTablets;

      } finally {
        if (client != null)
          MasterClient.close(client);
      }

    }

  }

  private static String getMRStats() throws Exception {
    Configuration conf = CachedConfiguration.getInstance();
    // No alternatives for hadoop 20
    JobClient jc = new JobClient(new org.apache.hadoop.mapred.JobConf(conf));

    ClusterStatus cs = jc.getClusterStatus(false);

    return "" + cs.getMapTasks() + " " + cs.getMaxMapTasks() + " " + cs.getReduceTasks() + " " + cs.getMaxReduceTasks() + " " + cs.getTaskTrackers() + " "
        + cs.getBlacklistedTrackers();

  }

  static class Opts extends ClientOnRequiredTable {}

  public static void main(String[] args) {
    Opts opts = new Opts();
    ScannerOpts scanOpts = new ScannerOpts();
    opts.parseArgs(ContinuousStatsCollector.class.getName(), args, scanOpts);
    Timer jtimer = new Timer();

    jtimer.schedule(new StatsCollectionTask(opts, scanOpts.scanBatchSize), 0, 30000);
  }

}
