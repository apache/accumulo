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
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TestDualAssignment extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.MANAGER_RECOVERY_DELAY, "5s");
    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void test() throws Exception {
    // make some tablets, spread 'em around
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      ServerContext serverContext = cluster.getServerContext();
      String table = this.getUniqueNames(1)[0];

      SortedSet<Text> partitions = new TreeSet<>();
      for (String part : "a b c d e f g h i j k l m n o p q r s t u v w x y z".split(" ")) {
        partitions.add(new Text(part));
      }
      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(partitions)
          .withInitialTabletAvailability(TabletAvailability.HOSTED);
      c.tableOperations().create(table, ntc);

      var tableId = serverContext.getTableId(table);
      var extent1 = new KeyExtent(tableId, new Text("m"), new Text("l"));

      var loc1 = TabletMetadata.Location.current("192.168.1.1:9997", "56");
      var loc2 = TabletMetadata.Location.future("192.168.1.2:9997", "67");

      // set multiple locations for a tablet
      serverContext.getAmple().mutateTablet(extent1).putLocation(loc1).putLocation(loc2).mutate();

      // This operation will fail when there are two locations set on a tablet
      assertThrows(AccumuloException.class, () -> c.tableOperations().setTabletAvailability(table,
          new Range(), TabletAvailability.HOSTED));

      try (var scanner = c.createScanner(table)) {
        // should not be able to scan the table when a tablet has multiple locations
        assertThrows(IllegalStateException.class, () -> scanner.stream().count());
      }

      // The following check ensures that Ample throws an exception when a tablet has multiple
      // locations and the location is being read.
      try (var tabletsMeta = serverContext.getAmple().readTablets().forTable(tableId).build()) {
        assertThrows(IllegalStateException.class, () -> tabletsMeta.stream().count());
      }

      // should fail when reading an individual tablet
      assertThrows(IllegalStateException.class, () -> serverContext.getAmple().readTablet(extent1));
      // when not reading a tablets location, ample should not fail
      assertEquals(extent1, serverContext.getAmple()
          .readTablet(extent1, TabletMetadata.ColumnType.FILES).getExtent());

      // fix the tablet metadata
      serverContext.getAmple().mutateTablet(extent1).deleteLocation(loc1).deleteLocation(loc2)
          .mutate();

      try (var scanner = c.createScanner(table)) {
        assertEquals(0, scanner.stream().count());
      }

      // should be able to scan using ample now
      try (var tabletsMeta = serverContext.getAmple().readTablets().forTable(tableId).build()) {
        assertEquals(27, tabletsMeta.stream().count());
      }

      // this should no longer fail
      c.tableOperations().setTabletAvailability(table, new Range(), TabletAvailability.HOSTED);
    }
  }
}
