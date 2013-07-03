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

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class BinaryStressIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "50K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "0");
    cfg.setSiteConfig(siteConfig );
  }

  @Test(timeout=60*1000)
  public void binaryStressTest() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("bt");
    c.tableOperations().setProperty("bt", Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    BinaryIT.runTest(c);
    String id = c.tableOperations().tableIdMap().get("bt");
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    FileStatus[] dir = fs.listStatus(new Path(cluster.getConfig().getDir() + "/accumulo/tables/" + id));
    assertTrue(dir.length  > 7);
  }
  
}
