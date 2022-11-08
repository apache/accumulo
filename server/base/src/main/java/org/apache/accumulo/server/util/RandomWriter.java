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
package org.apache.accumulo.server.util;

import java.security.SecureRandom;
import java.util.Iterator;
import java.util.Properties;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class RandomWriter {

  private static int num_columns_per_row = 1;
  private static int num_payload_bytes = 1024;
  private static final Logger log = LoggerFactory.getLogger(RandomWriter.class);
  private static final SecureRandom random = new SecureRandom();

  public static class RandomMutationGenerator implements Iterable<Mutation>, Iterator<Mutation> {
    private long max_mutations;
    private int mutations_so_far = 0;
    private static final Logger log = LoggerFactory.getLogger(RandomMutationGenerator.class);

    public RandomMutationGenerator(long num_mutations) {
      max_mutations = num_mutations;
    }

    @Override
    public boolean hasNext() {
      return mutations_so_far < max_mutations;
    }

    @Override
    public Mutation next() {
      Text row_value = new Text(
          Long.toString(((random.nextLong() & 0x7fffffffffffffffL) / 177) % 100000000000L));
      Mutation m = new Mutation(row_value);
      for (int column = 0; column < num_columns_per_row; column++) {
        Text column_fam = new Text("col_fam");
        byte[] bytes = new byte[num_payload_bytes];
        random.nextBytes(bytes);
        m.put(column_fam, new Text("" + column), new Value(bytes));
      }
      mutations_so_far++;
      if (mutations_so_far % 1000000 == 0) {
        log.info("Created {} mutations so far", mutations_so_far);
      }
      return m;
    }

    @Override
    public void remove() {
      mutations_so_far++;
    }

    @Override
    public Iterator<Mutation> iterator() {
      return this;
    }
  }

  static class Opts extends ClientOpts {
    @Parameter(names = "--count", description = "number of mutations to write", required = true)
    long count;
    @Parameter(names = "--table", description = "table to use")
    String tableName = "test_write_table";
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.principal = "root";
    opts.parseArgs(RandomWriter.class.getName(), args);

    Span span = TraceUtil.startSpan(RandomWriter.class, "main");
    try (Scope scope = span.makeCurrent()) {
      long start = System.currentTimeMillis();
      Properties clientProps = opts.getClientProps();
      String principal = ClientProperty.AUTH_PRINCIPAL.getValue(clientProps);
      log.info("starting at {} for user {}", start, principal);
      try (AccumuloClient accumuloClient = Accumulo.newClient().from(clientProps).build();
          BatchWriter bw = accumuloClient.createBatchWriter(opts.tableName)) {
        log.info("Writing {} mutations...", opts.count);
        bw.addMutations(new RandomMutationGenerator(opts.count));
      } catch (Exception e) {
        log.error("{}", e.getMessage(), e);
        throw e;
      }
      long stop = System.currentTimeMillis();

      log.info("stopping at {}", stop);
      log.info("elapsed: {}", (((double) stop - (double) start) / 1000.0));
    } finally {
      span.end();
    }
  }

}
