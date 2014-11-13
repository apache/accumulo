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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Do a selection of ITs with SSL turned on that cover a range of different connection scenarios. Note that you can run *all* the ITs against SSL-enabled mini
 * clusters with `mvn verify -DuseSslForIT`
 *
 */
public class SslIT extends ConfigurableMacIT {
  @Override
  public int defaultTimeoutSeconds() {
    return 6 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    super.configure(cfg, hadoopCoreSite);
    configureForSsl(cfg, createSharedTestDir(this.getClass().getName() + "-ssl"));
  }

  @Test
  public void binary() throws AccumuloException, AccumuloSecurityException, Exception {
    String tableName = getUniqueNames(1)[0];
    getConnector().tableOperations().create(tableName);
    BinaryIT.runTest(getConnector(), tableName);
  }

  @Test
  public void concurrency() throws Exception {
    ConcurrencyIT.runTest(getConnector(), getUniqueNames(1)[0]);
  }

  @Test
  public void adminStop() throws Exception {
    ShutdownIT.runAdminStopTest(getConnector(), getCluster());
  }

  @Test
  public void bulk() throws Exception {
    BulkIT.runTest(getConnector(), getUniqueNames(1)[0], this.getClass().getName(), testName.getMethodName());
  }

  @Test
  public void mapReduce() throws Exception {
    MapReduceIT.runTest(getConnector(), getCluster());
  }

}
