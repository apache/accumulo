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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.tabletserver.thrift.TDurability;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.constraints.NumericValueConstraint;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TServiceClient;
import org.junit.jupiter.api.Test;

public class BatchWriterIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(30);
  }

  @Test
  public void test() throws Exception {
    // call the batchwriter with buffer of size zero
    String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(table);
      BatchWriterConfig config = new BatchWriterConfig();
      config.setMaxMemory(0);
      try (BatchWriter writer = c.createBatchWriter(table, config)) {
        Mutation m = new Mutation("row");
        m.put("cf", "cq", new Value("value"));
        writer.addMutation(m);
      }
    }
  }

  private static void update(ClientContext context, Mutation m, KeyExtent extent) throws Exception {

    TabletLocator.TabletLocation tabLoc = TabletLocator.getLocator(context, extent.tableId())
        .locateTablet(context, new Text(m.getRow()), false, true);

    var server = HostAndPort.fromString(tabLoc.tablet_location);

    TabletClientService.Iface client = null;
    try {
      client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, server, context);
      client.update(TraceUtil.traceInfo(), context.rpcCreds(), extent.toThrift(), m.toThrift(),
          TDurability.DEFAULT);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code);
    } finally {
      ThriftUtil.returnClient((TServiceClient) client, context);
    }
  }

  static String toString(Map.Entry<Key,Value> e) {
    return e.getKey().getRow() + ":" + e.getKey().getColumnFamily() + ":"
        + e.getKey().getColumnQualifier() + ":" + e.getKey().getColumnVisibility() + ":"
        + e.getValue();
  }

  @Test
  public void testSingleMutationWriteRPC() throws Exception {
    // The batchwriter used to use this RPC and no longer does. This test exist to exercise the
    // unused RPC until its removed in 3.x. Older client versions of Accumulo 2.1.x may call this
    // RPC.

    String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.setProperties(Map.of(Property.TABLE_CONSTRAINT_PREFIX.getKey() + "1",
          NumericValueConstraint.class.getName()));
      c.tableOperations().create(table, ntc);

      var tableId = TableId.of(c.tableOperations().tableIdMap().get(table));

      Mutation m = new Mutation("r1");
      m.put("f1", "q3", new Value("1"));
      m.put("f1", "q4", new Value("2"));

      update((ClientContext) c, m, new KeyExtent(tableId, null, null));

      try (var scanner = c.createScanner(table)) {
        var entries = scanner.stream().map(BatchWriterIT::toString).collect(Collectors.toList());
        assertEquals(List.of("r1:f1:q3::1", "r1:f1:q4::2"), entries);
      }

      m = new Mutation("r1");
      m.put("f1", "q3", new Value("5"));
      m.put("f1", "q7", new Value("3"));

      update((ClientContext) c, m, new KeyExtent(tableId, null, null));

      try (var scanner = c.createScanner(table)) {
        var entries = scanner.stream().map(BatchWriterIT::toString).collect(Collectors.toList());
        assertEquals(List.of("r1:f1:q3::5", "r1:f1:q4::2", "r1:f1:q7::3"), entries);
      }

      var m2 = new Mutation("r2");
      m2.put("f1", "q1", new Value("abc"));
      assertThrows(ConstraintViolationException.class,
          () -> update((ClientContext) c, m2, new KeyExtent(tableId, null, null)));
    }

  }
}
