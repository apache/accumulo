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

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.junit.jupiter.api.Test;

public class RenameIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Test
  public void renameTest() throws Exception {
    String[] tableNames = getUniqueNames(2);
    String name1 = tableNames[0];
    String name2 = tableNames[1];
    VerifyParams params = new VerifyParams(cluster.getClientProperties(), name1);
    params.createTable = true;

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      TestIngest.ingest(c, params);
      c.tableOperations().rename(name1, name2);
      TestIngest.ingest(c, params);
      params.tableName = name2;
      VerifyIngest.verifyIngest(c, params);
      c.tableOperations().delete(name1);
      c.tableOperations().rename(name2, name1);
      params.tableName = name1;
      VerifyIngest.verifyIngest(c, params);
      FunctionalTestUtils.assertNoDanglingFateLocks(getCluster());
    }
  }
}
