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

import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class CleanTmpIT extends ConfigurableMacIT {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> props = new HashMap<String,String>();
    props.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "3s");
    cfg.setSiteConfig(props);
    cfg.setNumTservers(1);
    cfg.useMiniDFS(true);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    // make a table
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    // write to it
    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("row");
    m.put("cf", "cq", "value");
    bw.addMutation(m);
    bw.close();

    // create a fake _tmp file in its directory
    String id = c.tableOperations().tableIdMap().get(tableName);
    FileSystem fs = getCluster().getFileSystem();
    Path tmp = new Path("/accumulo/tables/" + id + "/default_tablet/junk.rf_tmp");
    fs.create(tmp).close();
    for (ProcessReference tserver : getCluster().getProcesses().get(ServerType.TABLET_SERVER)) {
      getCluster().killProcess(ServerType.TABLET_SERVER, tserver);
    }
    getCluster().start();

    Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY);
    FunctionalTestUtils.count(scanner);
    assertFalse(fs.exists(tmp));
  }
}
