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

import java.util.Collections;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ProcessReference;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.junit.Test;

public class MasterFailoverIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    cfg.setSiteConfig(Collections.singletonMap(Property.INSTANCE_ZK_TIMEOUT.getKey(), "5s"));
  }

  @Test(timeout=30*1000)
  public void test() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    TestIngest.Opts opts = new TestIngest.Opts();
    TestIngest.ingest(c, opts, BWOPTS);
    for (ProcessReference master : cluster.getProcesses().get(ServerType.MASTER)) {
      cluster.killProcess(ServerType.MASTER, master);
    }
    // start up a new one
    Process p = cluster.exec(Master.class);
    // talk to it
    c.tableOperations().rename("test_ingest", "test_ingest2");
    try {
      VerifyIngest.Opts vopts = new VerifyIngest.Opts();
      vopts.tableName = "test_ingest2";
      VerifyIngest.verifyIngest(c, vopts, SOPTS);
    } finally {
      p.destroy();
    }
  }
}
