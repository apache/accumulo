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
package org.apache.accumulo.test.functional;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService.Client;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataMaxFilesIT extends AccumuloClusterIT {
  private static final Logger log = LoggerFactory.getLogger(MetadataMaxFilesIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "1");
    siteConfig.put(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), "10");
    cfg.setSiteConfig(siteConfig);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  private String majcDelay, maxOpenFiles;

  @Before
  public void alterConf() throws Exception {
    Connector conn = getConnector();
    Map<String,String> conf = conn.instanceOperations().getSystemConfiguration();
    majcDelay = conf.get(Property.TSERV_MAJC_DELAY.getKey());
    maxOpenFiles = conf.get(Property.TSERV_SCAN_MAX_OPENFILES.getKey());
  }

  @After
  public void restoreConf() throws Exception {
    Connector conn = getConnector();
    if (null != majcDelay) {
      conn.instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), majcDelay);
    }

    if (null != maxOpenFiles) {
      conn.instanceOperations().setProperty(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), maxOpenFiles);
    }
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    SortedSet<Text> splits = new TreeSet<Text>();
    for (int i = 0; i < 1000; i++) {
      splits.add(new Text(String.format("%03d", i)));
    }
    final String[] names = getUniqueNames(5);
    String splitThreshold = null;
    try {
      for (Entry<String,String> entry : c.tableOperations().getProperties(MetadataTable.NAME)) {
        if (Property.TABLE_SPLIT_THRESHOLD.getKey().equals(entry.getKey())) {
          log.info("Got original table split threshold for metadata table of {}", entry.getValue());
          splitThreshold = entry.getValue();
          break;
        }
      }
      c.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10000");
      for (int i = 0; i < 5; i++) {
        String tableName = names[i];
        log.info("Creating {}", tableName);
        c.tableOperations().create(tableName);
        log.info("adding splits");
        c.tableOperations().addSplits(tableName, splits);
        log.info("flushing");
        c.tableOperations().flush(MetadataTable.NAME, null, null, true);
        c.tableOperations().flush(RootTable.NAME, null, null, true);
      }
      UtilWaitThread.sleep(20 * 1000);
      log.info("shutting down");
      cluster.stop();
      log.info("starting up");
      cluster.start();

      UtilWaitThread.sleep(30 * 1000);

      while (true) {
        MasterMonitorInfo stats = null;
        Credentials creds = new Credentials(getPrincipal(), getToken());
        Client client = null;
        try {
          client = MasterClient.getConnectionWithRetry(c.getInstance());
          stats = client.getMasterStats(Tracer.traceInfo(), creds.toThrift(c.getInstance()));
        } finally {
          if (client != null)
            MasterClient.close(client);
        }
        int tablets = 0;
        for (TabletServerStatus tserver : stats.tServerInfo) {
          for (Entry<String,TableInfo> entry : tserver.tableMap.entrySet()) {
            if (entry.getKey().startsWith("!"))
              continue;
            log.info("Found {} tablets for {}", entry.getValue().onlineTablets, entry.getKey());
            tablets += entry.getValue().onlineTablets;
          }
        }
        if (tablets >= 5005)
          break;
        log.info("Only found {} tablets, will retry", tablets);
        UtilWaitThread.sleep(1000);
      }
    } finally {
      if (null != splitThreshold) {
        c.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(), splitThreshold);
      }
    }
  }
}
