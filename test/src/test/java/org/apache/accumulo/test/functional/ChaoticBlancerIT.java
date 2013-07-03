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
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.server.master.balancer.ChaoticLoadBalancer;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ChaoticBlancerIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    Map<String,String> siteConfig = new HashMap<String, String>();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "10K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "0");
    siteConfig.put(Property.TABLE_LOAD_BALANCER.getKey(), ChaoticLoadBalancer.class.getName());
    cfg.setSiteConfig(siteConfig );
  }

  @Test(timeout=120*1000)
  public void test() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    SortedSet<Text> splits = new TreeSet<Text>();
    for (int i = 0; i < 200; i++) {
      splits.add(new Text(String.format("%03d", i)));
    }
    c.tableOperations().create("unused");
    c.tableOperations().addSplits("unused", splits);
    TestIngest.Opts opts = new TestIngest.Opts();
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.rows = opts.rows = 200000;
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    c.tableOperations().flush("test_ingest", null, null, true);
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
  }
  
}
