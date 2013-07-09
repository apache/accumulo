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

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ProcessReference;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.junit.Test;

public class WriteAheadLogIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    Map<String, String> siteConfig = new HashMap<String, String>();
    siteConfig.put(Property.TSERV_WALOG_MAX_SIZE.getKey(), "2M");
    siteConfig.put(Property.GC_CYCLE_DELAY.getKey(), "1");
    siteConfig.put(Property.GC_CYCLE_START.getKey(), "1");
    siteConfig.put(Property.MASTER_RECOVERY_DELAY.getKey(), "0");
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "200K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "1");
  }

  @Test(timeout=60*1000)
  public void test() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "750K");
    TestIngest.Opts opts = new TestIngest.Opts();
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    Map<ServerType,Collection<ProcessReference>> processes = cluster.getProcesses();
    for (ProcessReference tserver : processes.get(ServerType.TABLET_SERVER)) {
      cluster.killProcess(ServerType.TABLET_SERVER, tserver);
    }
    assertEquals(0, cluster.getProcesses().get(ServerType.TABLET_SERVER).size());
    cluster.start();
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    assertEquals(0, cluster.exec(Admin.class, "stopAll").waitFor());
  }
  
}
