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

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestRandomDeletes;
import org.apache.accumulo.test.VerifyIngest;
import org.junit.Test;

import com.google.common.base.Charsets;

public class DeleteIT extends AccumuloClusterIT {

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    PasswordToken token = (PasswordToken) getToken();
    deleteTest(c, getCluster(), new String(token.getPassword(), Charsets.UTF_8), tableName);
    try {
      getCluster().getClusterControl().adminStopAll();
    } finally {
      getCluster().start();
    }
  }

  public static void deleteTest(Connector c, AccumuloCluster cluster, String password, String tableName) throws Exception {
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    TestIngest.Opts opts = new TestIngest.Opts();
    vopts.tableName = opts.tableName = tableName;
    vopts.rows = opts.rows = 1000;
    vopts.cols = opts.cols = 1;
    vopts.random = opts.random = 56;
    BatchWriterOpts BWOPTS = new BatchWriterOpts();
    TestIngest.ingest(c, opts, BWOPTS);
    assertEquals(
        0,
        cluster.getClusterControl().exec(TestRandomDeletes.class,
            new String[] {"-u", "root", "-p", password, "-i", cluster.getInstanceName(), "-z", cluster.getZooKeepers(), "--table", tableName}));
    TestIngest.ingest(c, opts, BWOPTS);
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
  }

}
