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

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

public class AmpleIT extends AccumuloClusterHarness {

  private void runFetchTest(Ample ample, List<KeyExtent> extentsToFetch, Set<KeyExtent> expected,
      Set<KeyExtent> expectMissing) {
    Set<KeyExtent> extentsSeen;
    // always run a test without a consumer for not seen tablets as this takes a different code path
    try (TabletsMetadata tm =
        ample.readTablets().forTablets(extentsToFetch, Optional.empty()).build()) {
      extentsSeen = tm.stream().map(TabletMetadata::getExtent).collect(toSet());
      assertEquals(expected, extentsSeen);
    }

    HashSet<KeyExtent> extentsNotSeen = new HashSet<>();
    try (TabletsMetadata tm =
        ample.readTablets().forTablets(extentsToFetch, Optional.of(extentsNotSeen::add)).build()) {
      extentsSeen = tm.stream().map(TabletMetadata::getExtent).collect(toSet());
    }
    assertEquals(expected, extentsSeen);
    assertEquals(expectMissing, extentsNotSeen);
  }

  @Test
  public void testFetchMultipleExtents() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c"), new Text("f"), new Text("v")));
      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
      c.tableOperations().create(table, ntc);

      var tableId = TableId.of(c.tableOperations().tableIdMap().get(table));

      // extents that exist in the metadata table
      KeyExtent ke1 = new KeyExtent(tableId, new Text("c"), null);
      KeyExtent ke2 = new KeyExtent(tableId, new Text("f"), new Text("c"));
      KeyExtent ke3 = new KeyExtent(tableId, new Text("v"), new Text("f"));
      KeyExtent ke4 = new KeyExtent(tableId, null, new Text("v"));

      // extents that do not exist in the metadata table
      KeyExtent ne1 = new KeyExtent(tableId, null, new Text("g"));
      KeyExtent ne2 = new KeyExtent(tableId, new Text("e"), new Text("c"));
      KeyExtent ne3 = new KeyExtent(tableId, new Text("b"), null);
      KeyExtent ne4 = new KeyExtent(TableId.of(tableId.canonical() + "not"), new Text("c"), null);

      var ample = getServerContext().getAmple();

      var toFetch = new ArrayList<KeyExtent>();

      for (var existing : Sets.powerSet(Set.of(ke1, ke2, ke3, ke4))) {
        for (var nonexisting : Sets.powerSet(Set.of(ne1, ne2, ne3, ne4))) {
          toFetch.clear();
          toFetch.addAll(existing);
          toFetch.addAll(nonexisting);

          // run test to ensure when ample fetches multiple extents it handles one that do exist in
          // the metadata table and those that do not
          runFetchTest(ample, toFetch, existing, nonexisting);
        }
      }
    }
  }
}
