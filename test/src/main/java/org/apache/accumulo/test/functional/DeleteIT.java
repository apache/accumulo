/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestRandomDeletes;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.junit.jupiter.api.Test;

public class DeleteIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      deleteTest(c, getCluster(), tableName);
    }
  }

  public static void deleteTest(AccumuloClient c, AccumuloCluster cluster, String tableName)
      throws Exception {
    VerifyParams params = new VerifyParams(getClientProps(), tableName, 1000);
    params.cols = 1;
    params.random = 56;
    TestIngest.ingest(c, params);

    assertEquals(0, cluster.getClusterControl().exec(TestRandomDeletes.class,
        new String[] {"-c", cluster.getClientPropsPath(), "--table", tableName}));
    TestIngest.ingest(c, params);
    VerifyIngest.verifyIngest(c, params);
  }
}
