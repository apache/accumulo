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

import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.Refreshes.RefreshEntry;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

/**
 * Tests reading and writing refesh entries. This is done as a an IT instead of a unit test because
 * it would take too much code to mock ZK and the metadata table.
 */
public class RefreshesIT extends ConfigurableMacBase {
  private void testRefreshes(Ample.DataLevel level, KeyExtent extent1, KeyExtent extent2) {
    var refreshes = getServerContext().getAmple().refreshes(level);

    assertEquals(0, refreshes.stream().count());

    var ecid1 = ExternalCompactionId.generate(UUID.randomUUID());
    var ecid2 = ExternalCompactionId.generate(UUID.randomUUID());
    var ecid3 = ExternalCompactionId.generate(UUID.randomUUID());

    var tserver1 = new TServerInstance("host1:9997[abcdef123]");
    var tserver2 = new TServerInstance("host2:9997[1234567890]");

    refreshes.add(List.of(new RefreshEntry(ecid1, extent1, tserver1)));

    assertEquals(Map.of(ecid1, extent1),
        refreshes.stream().collect(toMap(RefreshEntry::getEcid, RefreshEntry::getExtent)));
    assertEquals(Map.of(ecid1, tserver1),
        refreshes.stream().collect(toMap(RefreshEntry::getEcid, RefreshEntry::getTserver)));

    refreshes.add(List.of(new RefreshEntry(ecid2, extent2, tserver1),
        new RefreshEntry(ecid3, extent2, tserver2)));

    assertEquals(Map.of(ecid1, extent1, ecid2, extent2, ecid3, extent2),
        refreshes.stream().collect(toMap(RefreshEntry::getEcid, RefreshEntry::getExtent)));
    assertEquals(Map.of(ecid1, tserver1, ecid2, tserver1, ecid3, tserver2),
        refreshes.stream().collect(toMap(RefreshEntry::getEcid, RefreshEntry::getTserver)));

    refreshes.delete(List.of(new RefreshEntry(ecid2, extent2, tserver1)));

    assertEquals(Map.of(ecid1, extent1, ecid3, extent2),
        refreshes.stream().collect(toMap(RefreshEntry::getEcid, RefreshEntry::getExtent)));
    assertEquals(Map.of(ecid1, tserver1, ecid3, tserver2),
        refreshes.stream().collect(toMap(RefreshEntry::getEcid, RefreshEntry::getTserver)));

    refreshes.delete(List.of(new RefreshEntry(ecid3, extent2, tserver2),
        new RefreshEntry(ecid1, extent1, tserver1)));

    assertEquals(0, refreshes.stream().count());
  }

  @Test
  public void testRefreshStorage() {
    var extent1 = new KeyExtent(TableId.of("1"), null, null);
    var extent2 = new KeyExtent(TableId.of("2"), new Text("m"), new Text("c"));

    // the root level only expects the root tablet extent
    assertThrows(IllegalArgumentException.class,
        () -> testRefreshes(Ample.DataLevel.ROOT, extent1, extent2));
    testRefreshes(Ample.DataLevel.ROOT, RootTable.EXTENT, RootTable.EXTENT);
    testRefreshes(Ample.DataLevel.METADATA, extent1, extent2);
    testRefreshes(Ample.DataLevel.USER, extent1, extent2);
  }
}
