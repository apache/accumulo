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

import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FateStarvationIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(FateStarvationIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    var groupName = "user_small";
    // Add this check in case the config changes
    Preconditions.checkState(
        Property.COMPACTION_SERVICE_DEFAULT_GROUPS.getDefaultValue().contains(groupName));
    // This test creates around ~1300 compaction task, so start more compactors. There is randomness
    // so the exact number of task varies.
    cfg.getClusterServerConfiguration().addCompactorResourceGroup(groupName, 4);
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
      log.debug("Ingest complete");

      c.tableOperations().flush(tableName, null, null, true);
      log.debug("Flush complete");

      List<Text> splits = new ArrayList<>(TestIngest.getSplitPoints(0, 100000, 67));

      List<Future<?>> futures = new ArrayList<>();
      var executor = Executors.newCachedThreadPool();

      for (int i = 0; i < 100; i++) {
        int idx1 = RANDOM.get().nextInt(splits.size() - 1);
        int idx2 = RANDOM.get().nextInt(splits.size() - (idx1 + 1)) + idx1 + 1;

        var future = executor.submit(() -> {
          c.tableOperations().compact(tableName, splits.get(idx1), splits.get(idx2), false, true);
          return null;
        });

        futures.add(future);
      }

      log.debug("Started compactions");

      // wait for all compactions to complete
      for (var future : futures) {
        future.get();
      }

      FunctionalTestUtils.assertNoDanglingFateLocks(getCluster());
    }
  }
}
