/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Do a selection of ITs with SSL turned on that cover a range of different connection scenarios.
 * Note that you can run *all* the ITs against SSL-enabled mini clusters with `mvn verify
 * -DuseSslForIT`
 */
public class SslIT extends ConfigurableMacBase {
  @Override
  public int defaultTimeoutSeconds() {
    return 6 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    super.configure(cfg, hadoopCoreSite);
    configureForSsl(cfg,
        getSslDir(createTestDir(this.getClass().getName() + "_" + this.testName.getMethodName())));
  }

  @Test
  public void binary() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      BinaryIT.runTest(client, tableName);
    }
  }

  @Test
  public void concurrency() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      ConcurrencyIT.runTest(client, getUniqueNames(1)[0]);
    }
  }

  @Test
  public void adminStop() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      ShutdownIT.runAdminStopTest(client, getCluster());
    }
  }

  @Test
  public void bulk() throws Exception {
    Properties props = getClientProperties();
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      BulkIT.runTest(client, ClientInfo.from(props), cluster.getFileSystem(),
          new Path(getCluster().getConfig().getDir().getAbsolutePath(), "tmp"),
          getUniqueNames(1)[0], this.getClass().getName(), testName.getMethodName(), true);
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void mapReduce() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // testing old mapreduce code from core jar; the new mapreduce module should have its own test
      // case which checks functionality with ssl enabled
      org.apache.accumulo.test.mapreduce.MapReduceIT.runTest(client, getCluster());
    }
  }

}
