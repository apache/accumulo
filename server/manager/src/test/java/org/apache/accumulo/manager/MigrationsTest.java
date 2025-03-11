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
package org.apache.accumulo.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.util.HostAndPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MigrationsTest {

  private final TServerInstance TS1 =
      new TServerInstance(HostAndPort.fromParts("server1", 9997), "s001");
  private final TServerInstance TS2 =
      new TServerInstance(HostAndPort.fromParts("server2", 1234), "s002");
  private final TServerInstance TS3 =
      new TServerInstance(HostAndPort.fromParts("server3", 6789), "s002");

  private final TableId TID1 = TableId.of("1");
  private final TableId TID2 = TableId.of("2");
  private final TableId TID3 = TableId.of("3");

  private final KeyExtent ME = new KeyExtent(MetadataTable.ID, null, null);

  private final KeyExtent KE1 = new KeyExtent(TID1, null, null);
  private final KeyExtent KE2 = new KeyExtent(TID2, null, null);
  private final KeyExtent KE3 = new KeyExtent(TID3, null, null);

  Migrations migrations;

  @BeforeEach
  public void setup() {
    migrations = new Migrations();
    migrations.put(RootTable.EXTENT, TS2);
    migrations.put(ME, TS1);
    migrations.put(KE1, TS2);
    migrations.put(KE2, TS1);
  }

  @Test
  public void testInitial() {
    assertEquals(4, migrations.size());
    assertEquals(Set.of(RootTable.EXTENT, ME, KE1, KE2), migrations.snapshotAll());
    assertEquals(Set.of(RootTable.EXTENT), migrations.snapshot(DataLevel.ROOT));
    assertEquals(Set.of(ME), migrations.snapshot(DataLevel.METADATA));
    assertEquals(Set.of(KE1, KE2), migrations.snapshot(DataLevel.USER));
    assertTrue(migrations.contains(RootTable.EXTENT));
    assertTrue(migrations.contains(ME));
    assertTrue(migrations.contains(KE1));
    assertTrue(migrations.contains(KE2));
    assertFalse(migrations.contains(KE3));
    assertEquals(TS2, migrations.get(RootTable.EXTENT));
    assertEquals(TS1, migrations.get(ME));
    assertEquals(TS2, migrations.get(KE1));
    assertEquals(TS1, migrations.get(KE2));
    assertNull(migrations.get(KE3));
    assertFalse(migrations.isEmpty());
    assertFalse(migrations.isEmpty(DataLevel.ROOT));
    assertFalse(migrations.isEmpty(DataLevel.METADATA));
    assertFalse(migrations.isEmpty(DataLevel.USER));
    assertEquals(Map.of(DataLevel.ROOT, Set.of(RootTable.EXTENT), DataLevel.METADATA, Set.of(ME),
        DataLevel.USER, Set.of(KE1, KE2)), migrations.mutableCopy());
  }

  @Test
  public void testRemoveServer() {
    migrations.removeServers(Set.of(TS1));

    assertEquals(2, migrations.size());
    assertEquals(Set.of(RootTable.EXTENT, KE1), migrations.snapshotAll());
    assertEquals(Set.of(RootTable.EXTENT), migrations.snapshot(DataLevel.ROOT));
    assertEquals(Set.of(), migrations.snapshot(DataLevel.METADATA));
    assertEquals(Set.of(KE1), migrations.snapshot(DataLevel.USER));
    assertTrue(migrations.contains(RootTable.EXTENT));
    assertFalse(migrations.contains(ME));
    assertTrue(migrations.contains(KE1));
    assertFalse(migrations.contains(KE3));
    assertEquals(TS2, migrations.get(RootTable.EXTENT));
    assertNull(migrations.get(ME));
    assertEquals(TS2, migrations.get(KE1));
    assertNull(migrations.get(KE2));
    assertNull(migrations.get(KE3));
    assertFalse(migrations.isEmpty());
    assertFalse(migrations.isEmpty(DataLevel.ROOT));
    assertTrue(migrations.isEmpty(DataLevel.METADATA));
    assertFalse(migrations.isEmpty(DataLevel.USER));
    assertEquals(Map.of(DataLevel.ROOT, Set.of(RootTable.EXTENT), DataLevel.METADATA, Set.of(),
        DataLevel.USER, Set.of(KE1)), migrations.mutableCopy());
  }

  @Test
  public void testRemoveExtent() {
    migrations.removeExtent(KE1);

    assertEquals(3, migrations.size());
    assertEquals(Set.of(RootTable.EXTENT, ME, KE2), migrations.snapshotAll());
    assertEquals(Set.of(RootTable.EXTENT), migrations.snapshot(DataLevel.ROOT));
    assertEquals(Set.of(ME), migrations.snapshot(DataLevel.METADATA));
    assertEquals(Set.of(KE2), migrations.snapshot(DataLevel.USER));
    assertTrue(migrations.contains(RootTable.EXTENT));
    assertTrue(migrations.contains(ME));
    assertFalse(migrations.contains(KE1));
    assertTrue(migrations.contains(KE2));
    assertFalse(migrations.contains(KE3));
    assertEquals(TS2, migrations.get(RootTable.EXTENT));
    assertEquals(TS1, migrations.get(ME));
    assertNull(migrations.get(KE1));
    assertEquals(TS1, migrations.get(KE2));
    assertNull(migrations.get(KE3));
    assertFalse(migrations.isEmpty());
    assertFalse(migrations.isEmpty(DataLevel.ROOT));
    assertFalse(migrations.isEmpty(DataLevel.METADATA));
    assertFalse(migrations.isEmpty(DataLevel.USER));
    assertEquals(Map.of(DataLevel.ROOT, Set.of(RootTable.EXTENT), DataLevel.METADATA, Set.of(ME),
        DataLevel.USER, Set.of(KE2)), migrations.mutableCopy());
  }

  @Test
  public void testRemoveExtents() {
    migrations.removeExtents(Set.of(KE1, RootTable.EXTENT));

    assertEquals(2, migrations.size());
    assertEquals(Set.of(ME, KE2), migrations.snapshotAll());
    assertEquals(Set.of(), migrations.snapshot(DataLevel.ROOT));
    assertEquals(Set.of(ME), migrations.snapshot(DataLevel.METADATA));
    assertEquals(Set.of(KE2), migrations.snapshot(DataLevel.USER));
    assertFalse(migrations.contains(RootTable.EXTENT));
    assertTrue(migrations.contains(ME));
    assertFalse(migrations.contains(KE1));
    assertTrue(migrations.contains(KE2));
    assertFalse(migrations.contains(KE3));
    assertNull(migrations.get(RootTable.EXTENT));
    assertEquals(TS1, migrations.get(ME));
    assertNull(migrations.get(KE1));
    assertEquals(TS1, migrations.get(KE2));
    assertNull(migrations.get(KE3));
    assertFalse(migrations.isEmpty());
    assertTrue(migrations.isEmpty(DataLevel.ROOT));
    assertFalse(migrations.isEmpty(DataLevel.METADATA));
    assertFalse(migrations.isEmpty(DataLevel.USER));
    assertEquals(Map.of(DataLevel.ROOT, Set.of(), DataLevel.METADATA, Set.of(ME), DataLevel.USER,
        Set.of(KE2)), migrations.mutableCopy());
  }

  @Test
  public void testRemoveTable() {
    migrations.removeTable(TID1);

    assertEquals(3, migrations.size());
    assertEquals(Set.of(RootTable.EXTENT, ME, KE2), migrations.snapshotAll());
    assertEquals(Set.of(RootTable.EXTENT), migrations.snapshot(DataLevel.ROOT));
    assertEquals(Set.of(ME), migrations.snapshot(DataLevel.METADATA));
    assertEquals(Set.of(KE2), migrations.snapshot(DataLevel.USER));
    assertTrue(migrations.contains(RootTable.EXTENT));
    assertTrue(migrations.contains(ME));
    assertFalse(migrations.contains(KE1));
    assertTrue(migrations.contains(KE2));
    assertFalse(migrations.contains(KE3));
    assertEquals(TS2, migrations.get(RootTable.EXTENT));
    assertEquals(TS1, migrations.get(ME));
    assertNull(migrations.get(KE1));
    assertEquals(TS1, migrations.get(KE2));
    assertNull(migrations.get(KE3));
    assertFalse(migrations.isEmpty());
    assertFalse(migrations.isEmpty(DataLevel.ROOT));
    assertFalse(migrations.isEmpty(DataLevel.METADATA));
    assertFalse(migrations.isEmpty(DataLevel.USER));
    assertEquals(Map.of(DataLevel.ROOT, Set.of(RootTable.EXTENT), DataLevel.METADATA, Set.of(ME),
        DataLevel.USER, Set.of(KE2)), migrations.mutableCopy());
  }

  @Test
  public void testEmpty() {
    migrations.removeServers(Set.of(TS1, TS2));

    assertEquals(0, migrations.size());
    assertEquals(Set.of(), migrations.snapshotAll());
    assertEquals(Set.of(), migrations.snapshot(DataLevel.ROOT));
    assertEquals(Set.of(), migrations.snapshot(DataLevel.METADATA));
    assertEquals(Set.of(), migrations.snapshot(DataLevel.USER));
    assertFalse(migrations.contains(RootTable.EXTENT));
    assertFalse(migrations.contains(ME));
    assertFalse(migrations.contains(KE1));
    assertFalse(migrations.contains(KE2));
    assertFalse(migrations.contains(KE3));
    assertNull(migrations.get(RootTable.EXTENT));
    assertNull(migrations.get(ME));
    assertNull(migrations.get(KE1));
    assertNull(migrations.get(KE2));
    assertNull(migrations.get(KE3));
    assertTrue(migrations.isEmpty());
    assertTrue(migrations.isEmpty(DataLevel.ROOT));
    assertTrue(migrations.isEmpty(DataLevel.METADATA));
    assertTrue(migrations.isEmpty(DataLevel.USER));
    assertEquals(
        Map.of(DataLevel.ROOT, Set.of(), DataLevel.METADATA, Set.of(), DataLevel.USER, Set.of()),
        migrations.mutableCopy());
  }
}
