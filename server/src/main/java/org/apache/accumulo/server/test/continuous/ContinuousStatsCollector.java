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
package org.apache.accumulo.server.test.continuous;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
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
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Stat;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.monitor.Monitor;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;

public class ContinuousStatsCollector {
  
  static class StatsCollectionTask extends TimerTask {
    
    private final String tableId;
    private ZooKeeperInstance instance;
    private String user;
    private String pass;
    
    public StatsCollectionTask(String tableName, String instanceName, String zooHosts, String user, String pass) {
      this.instance = new ZooKeeperInstance(instanceName, zooHosts);
      this.tableId = Tables.getNameToIdMap(instance).get(tableName);
      this.user = user;
      this.pass = pass;
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
      Connector conn = instance.getConnector(user, pass);
      
      Scanner scanner = conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
      scanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
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
      FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
      Path acudir = new Path(ServerConstants.getTablesDir());
      ContentSummary contentSummary = fs.getContentSummary(acudir);
      
      Path tableDir = new Path(ServerConstants.getTablesDir() + "/" + tableId);
      ContentSummary contentSummary2 = fs.getContentSummary(tableDir);
      
      return "" + contentSummary.getLength() + " " + contentSummary.getDirectoryCount() + " " + contentSummary.getFileCount() + " "
          + contentSummary2.getLength() + " " + contentSummary2.getDirectoryCount() + " " + contentSummary2.getFileCount();
    }
    
    private String getACUStats() throws Exception {
      
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(HdfsZooInstance.getInstance());
        MasterMonitorInfo stats = client.getMasterStats(null, SecurityConstants.getSystemCredentials());
        
        TableInfo all = new TableInfo();
        Map<String,TableInfo> tableSummaries = new HashMap<String,TableInfo>();
        
        for (TabletServerStatus server : stats.tServerInfo) {
          for (Entry<String,TableInfo> info : server.tableMap.entrySet()) {
            TableInfo tableSummary = tableSummaries.get(info.getKey());
            if (tableSummary == null) {
              tableSummary = new TableInfo();
              tableSummaries.put(info.getKey(), tableSummary);
            }
            Monitor.add(tableSummary, info.getValue());
            Monitor.add(all, info.getValue());
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
    @SuppressWarnings("deprecation")
    // No alternatives for hadoop 20
    JobClient jc = new JobClient(new org.apache.hadoop.mapred.JobConf(conf));
    
    ClusterStatus cs = jc.getClusterStatus(false);
    
    return "" + cs.getMapTasks() + " " + cs.getMaxMapTasks() + " " + cs.getReduceTasks() + " " + cs.getMaxReduceTasks() + " " + cs.getTaskTrackers() + " "
        + cs.getBlacklistedTrackers();
    
  }
  
  public static void main(String[] args) {
    Timer jtimer = new Timer();
    
    jtimer.schedule(new StatsCollectionTask(args[0], args[1], args[2], args[3], args[4]), 0, 30000);
  }
  
}
