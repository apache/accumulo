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

import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.metadata.ConditionalTabletsMutatorImpl;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class AmpleConditionalWriterIT extends AccumuloClusterHarness {

  Location newLocation(TServerInstance ts, LocationType lt) {
    return new Location(ts.getHostPort(), ts.getSession(), lt);
  }

  @Test
  public void testLocations() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = "testacw1";

      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c"), new Text("f"), new Text("j")));
      c.tableOperations().create(tableName,
          new NewTableConfiguration().withSplits(splits).createOffline());

      // TODO need to ungrant?
      c.securityOperations().grantTablePermission("root", MetadataTable.NAME,
          TablePermission.WRITE);

      var tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      var e1 = new KeyExtent(tid, new Text("c"), null);

      var ts1 = new TServerInstance("localhost:9997", 5000L);
      var ts2 = new TServerInstance("localhost:9997", 6000L);

      var context = (ClientContext) c;

      Assert.assertNull(context.getAmple().readTablet(e1).getLocation());

      var ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentLocation().putLocation(ts1, LocationType.FUTURE).submit();
      var results = ctmi.process();
      Assert.assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      Assert.assertEquals(newLocation(ts1, LocationType.FUTURE),
          context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentLocation().putLocation(ts2, LocationType.FUTURE).submit();
      results = ctmi.process();
      Assert.assertEquals(Status.REJECTED, results.get(e1).getStatus());

      Assert.assertEquals(newLocation(ts1, LocationType.FUTURE),
          context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireLocation(ts1, LocationType.FUTURE)
          .putLocation(ts1, LocationType.CURRENT).deleteLocation(ts1, LocationType.FUTURE).submit();
      results = ctmi.process();
      Assert.assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      Assert.assertEquals(newLocation(ts1, LocationType.CURRENT),
          context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireLocation(ts1, LocationType.FUTURE)
          .putLocation(ts1, LocationType.CURRENT).deleteLocation(ts1, LocationType.FUTURE).submit();
      results = ctmi.process();
      Assert.assertEquals(Status.REJECTED, results.get(e1).getStatus());

      Assert.assertEquals(newLocation(ts1, LocationType.CURRENT),
          context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireLocation(ts2, LocationType.FUTURE)
          .putLocation(ts2, LocationType.CURRENT).deleteLocation(ts2, LocationType.FUTURE).submit();
      results = ctmi.process();
      Assert.assertEquals(Status.REJECTED, results.get(e1).getStatus());

      Assert.assertEquals(newLocation(ts1, LocationType.CURRENT),
          context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireLocation(ts1, LocationType.CURRENT)
          .deleteLocation(ts1, LocationType.CURRENT).submit();
      results = ctmi.process();
      Assert.assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      Assert.assertNull(context.getAmple().readTablet(e1).getLocation());
    }
  }

  @Test
  public void testFiles() throws Exception {
    // TODO make test less verbose if keeping
    // TODO assert more results if keeping

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = "testacw2";

      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c"), new Text("f"), new Text("j")));
      c.tableOperations().create(tableName,
          new NewTableConfiguration().withSplits(splits).createOffline());

      // TODO need to ungrant?
      c.securityOperations().grantTablePermission("root", MetadataTable.NAME,
          TablePermission.WRITE);

      var tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      var e1 = new KeyExtent(tid, new Text("c"), null);

      var ts1 = new TServerInstance("localhost:9997", 5000L);
      var ts2 = new TServerInstance("localhost:9997", 6000L);

      var context = (ClientContext) c;

      var stf1 = new StoredTabletFile(
          "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf");
      var stf2 = new StoredTabletFile(
          "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000071.rf");
      var stf3 = new StoredTabletFile(
          "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000072.rf");
      var stf4 = new StoredTabletFile(
          "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/C0000073.rf");

      System.out.println(context.getAmple().readTablet(e1).getLocation());

      // simulate a compaction where the tablet location is not set
      var ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireFile(stf1).requireFile(stf2).requireFile(stf3)
          .putFile(stf4, new DataFileValue(0, 0)).submit();
      var results = ctmi.process();
      Assert.assertEquals(Status.REJECTED, results.get(e1).getStatus());

      Assert.assertEquals(Set.of(), context.getAmple().readTablet(e1).getFiles());

      // simulate minor compacts where the tablet location is not set
      for (StoredTabletFile file : List.of(stf1, stf2, stf3)) {
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(e1).requireLocation(ts1, LocationType.CURRENT)
            .putFile(file, new DataFileValue(0, 0)).submit();
        results = ctmi.process();
        Assert.assertEquals(Status.REJECTED, results.get(e1).getStatus());
      }

      Assert.assertEquals(Set.of(), context.getAmple().readTablet(e1).getFiles());

      // set the location
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentLocation().putLocation(ts1, LocationType.CURRENT).submit();
      results = ctmi.process();
      Assert.assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      // simulate minor compacts where the tablet location is wrong
      for (StoredTabletFile file : List.of(stf1, stf2, stf3)) {
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(e1).requireLocation(ts2, LocationType.CURRENT)
            .putFile(file, new DataFileValue(0, 0)).submit();
        results = ctmi.process();
        Assert.assertEquals(Status.REJECTED, results.get(e1).getStatus());
      }

      Assert.assertEquals(Set.of(), context.getAmple().readTablet(e1).getFiles());

      // simulate minor compacts where the tablet location is set
      for (StoredTabletFile file : List.of(stf1, stf2, stf3)) {
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(e1).requireLocation(ts1, LocationType.CURRENT)
            .putFile(file, new DataFileValue(0, 0)).submit();
        results = ctmi.process();
        Assert.assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      }

      Assert.assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());

      // simulate a compaction
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireFile(stf1).requireFile(stf2).requireFile(stf3)
          .putFile(stf4, new DataFileValue(0, 0)).deleteFile(stf1).deleteFile(stf2).deleteFile(stf3)
          .submit();
      results = ctmi.process();
      Assert.assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      Assert.assertEquals(Set.of(stf4), context.getAmple().readTablet(e1).getFiles());
    }
  }

  @Test
  public void testMultipleExtents() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = "testacw3";

      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c"), new Text("f"), new Text("j")));
      c.tableOperations().create(tableName,
          new NewTableConfiguration().withSplits(splits).createOffline());

      // TODO need to ungrant?
      c.securityOperations().grantTablePermission("root", MetadataTable.NAME,
          TablePermission.WRITE);

      var tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      var e1 = new KeyExtent(tid, new Text("c"), null);
      var e2 = new KeyExtent(tid, new Text("f"), new Text("c"));
      var e3 = new KeyExtent(tid, new Text("j"), new Text("f"));
      var e4 = new KeyExtent(tid, null, new Text("j"));

      var ts1 = new TServerInstance("localhost:9997", 5000L);
      var ts2 = new TServerInstance("localhost:9997", 6000L);

      var context = (ClientContext) c;

      var ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentLocation().putLocation(ts1, LocationType.FUTURE).submit();
      ctmi.mutateTablet(e2).requireAbsentLocation().putLocation(ts2, LocationType.FUTURE).submit();
      var results = ctmi.process();

      Assert.assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      Assert.assertEquals(Status.ACCEPTED, results.get(e2).getStatus());

      Assert.assertEquals(newLocation(ts1, LocationType.FUTURE),
          context.getAmple().readTablet(e1).getLocation());
      Assert.assertEquals(newLocation(ts2, LocationType.FUTURE),
          context.getAmple().readTablet(e2).getLocation());
      Assert.assertNull(context.getAmple().readTablet(e3).getLocation());
      Assert.assertNull(context.getAmple().readTablet(e4).getLocation());

      Assert.assertEquals(Set.of(e1, e2), results.keySet());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentLocation().putLocation(ts2, LocationType.FUTURE).submit();
      ctmi.mutateTablet(e2).requireAbsentLocation().putLocation(ts1, LocationType.FUTURE).submit();
      ctmi.mutateTablet(e3).requireAbsentLocation().putLocation(ts1, LocationType.FUTURE).submit();
      ctmi.mutateTablet(e4).requireAbsentLocation().putLocation(ts2, LocationType.FUTURE).submit();
      results = ctmi.process();

      Assert.assertEquals(Status.REJECTED, results.get(e1).getStatus());
      Assert.assertEquals(Status.REJECTED, results.get(e2).getStatus());
      Assert.assertEquals(Status.ACCEPTED, results.get(e3).getStatus());
      Assert.assertEquals(Status.ACCEPTED, results.get(e4).getStatus());

      Assert.assertEquals(newLocation(ts1, LocationType.FUTURE),
          context.getAmple().readTablet(e1).getLocation());
      Assert.assertEquals(newLocation(ts2, LocationType.FUTURE),
          context.getAmple().readTablet(e2).getLocation());
      Assert.assertEquals(newLocation(ts1, LocationType.FUTURE),
          context.getAmple().readTablet(e3).getLocation());
      Assert.assertEquals(newLocation(ts2, LocationType.FUTURE),
          context.getAmple().readTablet(e4).getLocation());

      Assert.assertEquals(Set.of(e1, e2, e3, e4), results.keySet());

    }
  }

}
