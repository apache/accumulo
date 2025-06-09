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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MINI_CLUSTER_ONLY)
public class ThriftMaxFrameSize2IT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.RPC_MAX_MESSAGE_SIZE, "256K");
    cfg.setProperty(Property.RPC_MAX_TOTAL_READ_SIZE, "1M");
  }

  @Test
  public void testMaxTotalMaxMessages() throws Exception {
    String table = getUniqueNames(1)[0];
    var executor = Executors.newCachedThreadPool();
    try (var client = Accumulo.newClient().from(getClientProperties()).build()) {
      client.tableOperations().create(table);

      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < 500; i++) {
        String row = "bigvalue" + i;
        var future = executor.submit(() -> {
          try (var writer = client.createBatchWriter(table)) {
            Mutation m = new Mutation("bigvalue");
            m.at().family("data").qualifier("1").put(new byte[128_000]);
            writer.addMutation(m);
          }

          return null;
        });
        futures.add(future);
      }

      for (var future : futures) {
        future.get();
      }
      /*
       * try(var writer = client.createBatchWriter(table)) { Mutation m = new Mutation("bigvalue");
       * m.at().family("data").qualifier("1").put(new byte[512_000]); // TODO this write will fail
       * and retry forever because it exceeds the size of individual message, can we detect this and
       * percolate an exception back up? writer.addMutation(m); }
       */
    }
  }

}
