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

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.cluster.ClusterServerType;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BinaryStressIT extends AccumuloClusterIT {

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "50K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "0");
    cfg.setSiteConfig(siteConfig);
  }

  private String majcDelay, maxMem;

  @Before
  public void alterConfig() throws Exception {
    if (ClusterType.MINI == getClusterType()) {
      return;
    }

    InstanceOperations iops = getConnector().instanceOperations();
    Map<String,String> conf = iops.getSystemConfiguration();
    majcDelay = conf.get(Property.TSERV_MAJC_DELAY.getKey());
    maxMem = conf.get(Property.TSERV_MAXMEM.getKey());

    iops.setProperty(Property.TSERV_MAJC_DELAY.getKey(), "0");
    iops.setProperty(Property.TSERV_MAXMEM.getKey(), "50K");

    getClusterControl().stopAllServers(ClusterServerType.TABLET_SERVER);
    getClusterControl().startAllServers(ClusterServerType.TABLET_SERVER);
  }

  @After
  public void resetConfig() throws Exception {
    if (null != majcDelay) {
      InstanceOperations iops = getConnector().instanceOperations();
      iops.setProperty(Property.TSERV_MAJC_DELAY.getKey(), majcDelay);
      iops.setProperty(Property.TSERV_MAXMEM.getKey(), maxMem);

      getClusterControl().stopAllServers(ClusterServerType.TABLET_SERVER);
      getClusterControl().startAllServers(ClusterServerType.TABLET_SERVER);
    }
  }

  @Test
  public void binaryStressTest() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    BinaryIT.runTest(c, tableName);
    String id = c.tableOperations().tableIdMap().get(tableName);
    Set<Text> tablets = new HashSet<Text>();
    Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.setRange(Range.prefix(id));
    for (Entry<Key,Value> entry : s) {
      tablets.add(entry.getKey().getRow());
    }
    assertTrue("Expected at least 8 tablets, saw " + tablets.size(), tablets.size() > 7);
  }

}
