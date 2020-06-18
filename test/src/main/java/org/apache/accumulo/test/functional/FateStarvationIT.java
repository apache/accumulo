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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * See ACCUMULO-779
 */
public class FateStarvationIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void run() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(tableName);

      c.tableOperations().addSplits(tableName, TestIngest.getSplitPoints(0, 100000, 50));

      IngestParams params = new IngestParams(getClientProps(), tableName, 100_000);
      params.random = 89;
      params.timestamp = 7;
      params.dataSize = 50;
      params.cols = 1;
      TestIngest.ingest(c, params);

      c.tableOperations().flush(tableName, null, null, true);

      List<Text> splits = new ArrayList<>(TestIngest.getSplitPoints(0, 100000, 67));
      Random rand = new SecureRandom();

      for (int i = 0; i < 100; i++) {
        int idx1 = rand.nextInt(splits.size() - 1);
        int idx2 = rand.nextInt(splits.size() - (idx1 + 1)) + idx1 + 1;

        c.tableOperations().compact(tableName, splits.get(idx1), splits.get(idx2), false, false);
      }

      c.tableOperations().offline(tableName);

      FunctionalTestUtils.assertNoDanglingFateLocks((ClientContext) c, getCluster());
    }
  }
}
