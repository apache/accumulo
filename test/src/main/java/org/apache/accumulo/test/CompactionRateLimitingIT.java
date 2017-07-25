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

import java.util.Random;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class CompactionRateLimitingIT extends ConfigurableMacBase {
  public static final long BYTES_TO_WRITE = 10 * 1024 * 1024;
  public static final long RATE = 1 * 1024 * 1024;

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration fsConf) {
    cfg.setProperty(Property.TSERV_MAJC_THROUGHPUT, RATE + "B");
    cfg.setProperty(Property.TABLE_MAJC_RATIO, "20");
    cfg.setProperty(Property.TABLE_FILE_COMPRESSION_TYPE, "none");
  }

  @Test
  public void majorCompactionsAreRateLimited() throws Exception {
    long bytesWritten = 0;
    String tableName = getUniqueNames(1)[0];
    Connector conn = getCluster().getConnector("root", new PasswordToken(ROOT_PASSWORD));
    conn.tableOperations().create(tableName);
    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    try {
      Random r = new Random();
      while (bytesWritten < BYTES_TO_WRITE) {
        byte[] rowKey = new byte[32];
        r.nextBytes(rowKey);

        byte[] qual = new byte[32];
        r.nextBytes(qual);

        byte[] value = new byte[1024];
        r.nextBytes(value);

        Mutation m = new Mutation(rowKey);
        m.put(new byte[0], qual, value);
        bw.addMutation(m);

        bytesWritten += rowKey.length + qual.length + value.length;
      }
    } finally {
      bw.close();
    }

    conn.tableOperations().flush(tableName, null, null, true);

    long compactionStart = System.currentTimeMillis();
    conn.tableOperations().compact(tableName, null, null, false, true);
    long duration = System.currentTimeMillis() - compactionStart;
    // The rate will be "bursty", try to account for that by taking 80% of the expected rate (allow for 20% under the maximum expected duration)
    Assert.assertTrue(
        String.format("Expected a compaction rate of no more than %,d bytes/sec, but saw a rate of %,f bytes/sec", (int) 0.8d * RATE, 1000.0 * bytesWritten
            / duration), duration > 1000L * 0.8 * BYTES_TO_WRITE / RATE);
  }
}
