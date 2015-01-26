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

import org.apache.accumulo.cluster.ClusterServerType;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class WriteAheadLogIT extends AccumuloClusterIT {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.TSERV_WALOG_MAX_SIZE.getKey(), "2M");
    siteConfig.put(Property.GC_CYCLE_DELAY.getKey(), "1");
    siteConfig.put(Property.GC_CYCLE_START.getKey(), "1");
    siteConfig.put(Property.MASTER_RECOVERY_DELAY.getKey(), "1s");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "1");
    siteConfig.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "4s");
    cfg.setSiteConfig(siteConfig);
    cfg.useMiniDFS(true);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 10 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "750K");
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.tableName = tableName;
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.tableName = tableName;
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    getCluster().getClusterControl().stopAllServers(ClusterServerType.TABLET_SERVER);
    getCluster().getClusterControl().startAllServers(ClusterServerType.TABLET_SERVER);
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
  }

}
