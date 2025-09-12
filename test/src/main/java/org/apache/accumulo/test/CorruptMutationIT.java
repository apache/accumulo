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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TMutation;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletingest.thrift.TDurability;
import org.apache.accumulo.core.tabletingest.thrift.TabletIngestClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TServiceClient;
import org.junit.jupiter.api.Test;

public class CorruptMutationIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX, "10");
  }

  @Test
  public void testCorruptMutation() throws Exception {

    String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(table);
      try (BatchWriter writer = c.createBatchWriter(table)) {
        Mutation m = new Mutation("1");
        m.put("f1", "q1", new Value("v1"));
        writer.addMutation(m);
      }

      var ctx = (ClientContext) c;
      var tableId = ctx.getTableId(table);
      var extent = new KeyExtent(tableId, null, null);
      var tabletMetadata = ctx.getAmple().readTablet(extent, TabletMetadata.ColumnType.LOCATION);
      var location = tabletMetadata.getLocation();
      assertNotNull(location);
      assertEquals(TabletMetadata.LocationType.CURRENT, location.getType());

      TabletIngestClientService.Iface client = ThriftUtil.getClient(ThriftClientTypes.TABLET_INGEST,
          location.getServerInstance().getServer(), ctx);
      // Make the same RPC calls made by the BatchWriter, but pass a corrupt serialized mutation in
      // this try block.
      try {
        TInfo tinfo = TraceUtil.traceInfo();

        long sessionId = client.startUpdate(tinfo, ctx.rpcCreds(), TDurability.DEFAULT);

        // Write two valid mutations to the session. The tserver buffers data it receives via
        // applyUpdates and may not write them until closeUpdate RPC is called. Because
        // TSERV_TOTAL_MUTATION_QUEUE_MAX was set so small, these values should be written.
        client.applyUpdates(tinfo, sessionId, extent.toThrift(),
            List.of(createTMutation("abc", "z1"), createTMutation("def", "z2")));

        // Simulate data corruption in the serialized mutation
        TMutation badMutation = createTMutation("ghi", "z3");
        badMutation.entries = -42;

        // Write some good and bad mutations to the session. The server side will see an error here,
        // however since this is a thrift oneway method no exception is expected here. This should
        // leave the session in a broken state where it no longer accepts any new data.
        client.applyUpdates(tinfo, sessionId, extent.toThrift(),
            List.of(createTMutation("jkl", "z4"), badMutation, createTMutation("mno", "z5")));

        // Write two more valid mutations to the session, these should be dropped on the server side
        // because of the previous error. So should never see these updates.
        client.applyUpdates(tinfo, sessionId, extent.toThrift(),
            List.of(createTMutation("pqr", "z6"), createTMutation("stu", "z7")));

        // Since client.applyUpdates experienced an error, should see an error when closing the
        // session.
        assertThrows(TApplicationException.class, () -> client.closeUpdate(tinfo, sessionId));
      } finally {
        ThriftUtil.returnClient((TServiceClient) client, ctx);
      }

      // The values that a scan must see
      Set<String> expectedValues = Set.of("v1", "v2", "z1", "z2");

      // The failed mutation should not have left the tablet in a bad state. Do some follow-on
      // actions to ensure the tablet is still functional.
      try (BatchWriter writer = c.createBatchWriter(table)) {
        Mutation m = new Mutation("2");
        m.put("f1", "q1", new Value("v2"));
        writer.addMutation(m);
      }

      try (Scanner scanner = c.createScanner(table)) {
        var valuesSeen =
            scanner.stream().map(e -> e.getValue().toString()).collect(Collectors.toSet());
        assertEquals(expectedValues, valuesSeen);
      }

      c.tableOperations().flush(table, null, null, true);

      try (Scanner scanner = c.createScanner(table)) {
        var valuesSeen =
            scanner.stream().map(e -> e.getValue().toString()).collect(Collectors.toSet());
        assertEquals(expectedValues, valuesSeen);
      }
    }
  }

  private static TMutation createTMutation(String row, String value) {
    Mutation m = new Mutation(row);
    m.put("x", "y", value);
    return m.toThrift();
  }
}
