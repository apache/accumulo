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
package org.apache.accumulo.test.compaction;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class CompactionRateLimitingIT extends ConfigurableMacBase {
  public static final long BYTES_TO_WRITE = 10 * 1024 * 1024;
  public static final long RATE = 1 * 1024 * 1024;

  protected Property getThroughputProp() {
    return Property.TSERV_COMPACTION_SERVICE_DEFAULT_RATE_LIMIT;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration fsConf) {
    cfg.setProperty(getThroughputProp(), RATE + "B");
    cfg.setProperty(Property.TABLE_MAJC_RATIO, "20");
    cfg.setProperty(Property.TABLE_FILE_COMPRESSION_TYPE, "none");

    cfg.setProperty("tserver.compaction.major.service.test.rate.limit", RATE + "B");
    cfg.setProperty("tserver.compaction.major.service.test.planner",
        DefaultCompactionPlanner.class.getName());
    cfg.setProperty("tserver.compaction.major.service.test.planner.opts.executors",
        "[{'name':'all','numThreads':2}]".replaceAll("'", "\""));

  }

  @Test
  public void majorCompactionsAreRateLimited() throws Exception {
    long bytesWritten = 0;
    String[] tableNames = getUniqueNames(1);

    try (AccumuloClient client =
        getCluster().createAccumuloClient("root", new PasswordToken(ROOT_PASSWORD))) {

      for (int i = 0; i < tableNames.length; i++) {
        String tableName = tableNames[i];

        NewTableConfiguration ntc = new NewTableConfiguration();
        if (i == 1) {
          ntc.setProperties(Map.of("table.compaction.dispatcher.opts.service", "test"));
        }

        client.tableOperations().create(tableName, ntc);
        try (BatchWriter bw = client.createBatchWriter(tableName)) {
          while (bytesWritten < BYTES_TO_WRITE) {
            byte[] rowKey = new byte[32];
            random.nextBytes(rowKey);

            byte[] qual = new byte[32];
            random.nextBytes(qual);

            byte[] value = new byte[1024];
            random.nextBytes(value);

            Mutation m = new Mutation(rowKey);
            m.put(new byte[0], qual, value);
            bw.addMutation(m);

            bytesWritten += rowKey.length + qual.length + value.length;
          }
        }

        client.tableOperations().flush(tableName, null, null, true);

        long compactionStart = System.currentTimeMillis();
        client.tableOperations().compact(tableName, null, null, false, true);
        long duration = System.currentTimeMillis() - compactionStart;
        // The rate will be "bursty", try to account for that by taking 80% of the expected rate
        // (allow for 20% under the maximum expected duration)
        String message = String.format(
            "Expected a compaction rate of no more than %,d bytes/sec, but saw a rate of %,f bytes/sec",
            (int) (0.8d * RATE), 1000.0 * bytesWritten / duration);
        assertTrue(duration > 1000L * 0.8 * BYTES_TO_WRITE / RATE, message);
      }
    }
  }
}
