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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.google.common.collect.Iterators;

// ACCUMULO-3914
public class TabletServerHdfsRestartIT extends ConfigurableMacBase {

  private static final int N = 1000;

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.useMiniDFS(true);
    cfg.setNumTservers(1);
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
  }

  @Test(timeout = 2 * 60 * 1000)
  public void test() throws Exception {
    final Connector conn = this.getConnector();
    // Yes, there's a tabletserver
    assertEquals(1, conn.instanceOperations().getTabletServers().size());
    final String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);
    BatchWriter bw = conn.createBatchWriter(tableName, null);
    for (int i = 0; i < N; i++) {
      Mutation m = new Mutation("" + i);
      m.put("", "", "");
      bw.addMutation(m);
    }
    bw.close();
    conn.tableOperations().flush(tableName, null, null, true);

    // Kill dfs
    cluster.getMiniDfs().restartNameNode(false);

    assertEquals(N, Iterators.size(conn.createScanner(tableName, Authorizations.EMPTY).iterator()));
  }

}
