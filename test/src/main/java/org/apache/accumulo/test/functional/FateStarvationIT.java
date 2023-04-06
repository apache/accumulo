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
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class FateStarvationIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Test
  public void run() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var ntc = new NewTableConfiguration().withSplits(TestIngest.getSplitPoints(0, 100000, 50));
      c.tableOperations().create(tableName, ntc);

      IngestParams params = new IngestParams(getClientProps(), tableName, 100_000);
      params.random = 89;
      params.timestamp = 7;
      params.dataSize = 50;
      params.cols = 1;
      TestIngest.ingest(c, params);

      c.tableOperations().flush(tableName, null, null, true);

      List<Text> splits = new ArrayList<>(TestIngest.getSplitPoints(0, 100000, 67));

      for (int i = 0; i < 100; i++) {
        int idx1 = random.nextInt(splits.size() - 1);
        int idx2 = random.nextInt(splits.size() - (idx1 + 1)) + idx1 + 1;

        c.tableOperations().compact(tableName, splits.get(idx1), splits.get(idx2), false, false);
      }

      c.tableOperations().offline(tableName);

      FunctionalTestUtils.assertNoDanglingFateLocks(getCluster());
    }
  }
}
