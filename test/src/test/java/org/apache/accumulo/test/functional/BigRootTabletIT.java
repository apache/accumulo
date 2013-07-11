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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.Test;

public class BigRootTabletIT extends MacTest {
  // ACCUMULO-542: A large root tablet will fail to load if it does't fit in the tserver scan buffers
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.TABLE_SCAN_MAXMEM.getKey(), "1024");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "60m");
    cfg.setSiteConfig(siteConfig);
  }
  
  @Test(timeout = 2 * 60 * 1000)
  public void test() throws Exception {
    Connector c = getConnector();
    c.tableOperations().addSplits(MetadataTable.NAME, FunctionalTestUtils.splits("0 1 2 3 4 5 6 7 8 9 a".split(" ")));
    for (int i = 0; i < 10; i++) {
      c.tableOperations().create("" + i);
      c.tableOperations().flush(MetadataTable.NAME, null, null, true);
      c.tableOperations().flush(RootTable.NAME, null, null, true);
    }
    cluster.stop();
    cluster.start();
    int count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : c.createScanner(RootTable.NAME, Authorizations.EMPTY))
      count++;
    assertTrue(count > 0);
  }
  
}
