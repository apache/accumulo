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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ZookeeperRestartIT extends ConfigurableMacIT {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "3s");
    cfg.setSiteConfig(siteConfig);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    BatchWriter bw = c.createBatchWriter("test_ingest", null);
    Mutation m = new Mutation("row");
    m.put("cf", "cq", "value");
    bw.addMutation(m);
    bw.close();

    // kill zookeeper
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.ZOOKEEPER))
      cluster.killProcess(ServerType.ZOOKEEPER, proc);

    // give the servers time to react
    UtilWaitThread.sleep(1000);

    // start zookeeper back up
    cluster.start();

    // use the tservers
    Scanner s = c.createScanner("test_ingest", Authorizations.EMPTY);
    Iterator<Entry<Key,Value>> i = s.iterator();
    assertTrue(i.hasNext());
    assertEquals("row", i.next().getKey().getRow().toString());
    assertFalse(i.hasNext());
    // use the master
    c.tableOperations().delete("test_ingest");
  }

}
