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
package org.apache.accumulo.test;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.tracer.TraceDump;
import org.apache.accumulo.tracer.TraceServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.Test;

public class TracerRecoversAfterOfflineTableIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    cfg.setNumTservers(1);
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void test() throws Exception {
    Process tracer = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      if (!client.tableOperations().exists("trace")) {
        MiniAccumuloClusterImpl mac = cluster;
        tracer = mac.exec(TraceServer.class).getProcess();
        while (!client.tableOperations().exists("trace")) {
          sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        sleepUninterruptibly(5, TimeUnit.SECONDS);
      }

      log.info("Taking table offline");
      client.tableOperations().offline("trace", true);

      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      log.info("Start a distributed trace span");

      TraceUtil.enableClientTraces("localhost", "testTrace", getClientProperties());
      long rootTraceId;
      try (TraceScope root = Trace.startSpan("traceTest", Sampler.ALWAYS)) {
        rootTraceId = root.getSpan().getTraceId();
        try (BatchWriter bw = client.createBatchWriter(tableName)) {
          Mutation m = new Mutation("m");
          m.put("a", "b", "c");
          bw.addMutation(m);
        }
      }

      log.info("Bringing trace table back online");
      client.tableOperations().online("trace", true);

      log.info("Trace table is online, should be able to find trace");

      try (Scanner scanner = client.createScanner("trace", Authorizations.EMPTY)) {
        scanner.setRange(new Range(new Text(Long.toHexString(rootTraceId))));
        while (true) {
          final StringBuilder finalBuffer = new StringBuilder();
          int traceCount = TraceDump.printTrace(scanner, line -> {
            try {
              finalBuffer.append(line).append("\n");
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
          });
          String traceOutput = finalBuffer.toString();
          log.info("Trace output:{}", traceOutput);
          if (traceCount > 0) {
            int lastPos = 0;
            for (String part : "traceTest,close,binMutations".split(",")) {
              log.info("Looking in trace output for '{}'", part);
              int pos = traceOutput.indexOf(part);
              assertTrue("Did not find '" + part + "' in output", pos > 0);
              assertTrue("'" + part + "' occurred earlier than the previous element unexpectedly",
                  pos > lastPos);
              lastPos = pos;
            }
            break;
          } else {
            log.info("Ignoring trace output as traceCount not greater than zero: {}", traceCount);
            Thread.sleep(1000);
          }
        }
        if (tracer != null) {
          tracer.destroy();
        }
      }
    }
  }

}
