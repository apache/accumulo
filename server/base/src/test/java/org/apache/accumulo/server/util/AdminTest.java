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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.hadoop.fs.Path;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class AdminTest {

  @Test
  public void testZooKeeperTserverPath() {
    ClientContext context = EasyMock.createMock(ClientContext.class);
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());

    EasyMock.expect(context.getZooKeeperRoot()).andReturn(Constants.ZROOT + "/" + instanceId);

    EasyMock.replay(context);

    assertEquals(Constants.ZROOT + "/" + instanceId + Constants.ZTSERVERS,
        Admin.getTServersZkPath(context));

    EasyMock.verify(context);
  }

  @Test
  public void testQualifySessionId() {
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    String root = "/accumulo/id/tservers";
    String server = "localhost:12345";
    final long session = 123456789L;

    String serverPath = root + "/" + server;
    String validZLockEphemeralNode = "zlock#" + UUID.randomUUID() + "#0000000000";
    EasyMock.expect(zc.getChildren(serverPath))
        .andReturn(Collections.singletonList(validZLockEphemeralNode));
    EasyMock.expect(zc.get(EasyMock.eq(serverPath + "/" + validZLockEphemeralNode),
        EasyMock.anyObject(ZcStat.class))).andAnswer(() -> {
          ZcStat stat = (ZcStat) EasyMock.getCurrentArguments()[1];
          stat.setEphemeralOwner(session);
          return new byte[0];
        });

    EasyMock.replay(zc);

    assertEquals(server + "[" + Long.toHexString(session) + "]",
        Admin.qualifyWithZooKeeperSessionId(root, zc, server));

    EasyMock.verify(zc);
  }

  @Test
  public void testCannotQualifySessionId() {
    ZooCache zc = EasyMock.createMock(ZooCache.class);

    String root = "/accumulo/id/tservers";
    String server = "localhost:12345";

    String serverPath = root + "/" + server;
    EasyMock.expect(zc.getChildren(serverPath)).andReturn(Collections.emptyList());

    EasyMock.replay(zc);

    // A server that isn't in ZooKeeper. Can't qualify it, should return the original
    assertEquals(server, Admin.qualifyWithZooKeeperSessionId(root, zc, server));

    EasyMock.verify(zc);
  }

  @Test
  public void testDanglingFate() {
    KeyExtent[] extents = new KeyExtent[10];
    for (int i = 0; i < extents.length; i++) {
      extents[i] = new KeyExtent(TableId.of("" + i), null, null);
    }

    FateId[] fateIds = new FateId[10];
    for (int i = 0; i < fateIds.length; i++) {
      fateIds[i] = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    }

    var opid1 = TabletOperationId.from(TabletOperationType.SPLITTING, fateIds[0]);
    var opid2 = TabletOperationId.from(TabletOperationType.MERGING, fateIds[1]);
    var opid3 = TabletOperationId.from(TabletOperationType.MERGING, fateIds[5]);

    var files = Set.of(StoredTabletFile.of(new Path("file:///accumulo/tables/4/t-1/f4.rf")));
    var sf1 =
        new SelectedFiles(files, true, fateIds[6], SteadyTime.from(100_100, TimeUnit.NANOSECONDS));
    var sf2 =
        new SelectedFiles(files, true, fateIds[7], SteadyTime.from(100_100, TimeUnit.NANOSECONDS));

    var tm1 = TabletMetadata.builder(extents[0]).putOperation(opid1).build(LOADED, SELECTED);
    var tm2 = TabletMetadata.builder(extents[1]).putOperation(opid2).build(LOADED, SELECTED);
    var tm3 = TabletMetadata.builder(extents[2])
        .putBulkFile(ReferencedTabletFile.of(new Path("file:///accumulo/tables/1/t-1/f1.rf")),
            fateIds[2])
        .build(OPID, SELECTED);
    var tm4 = TabletMetadata.builder(extents[3])
        .putBulkFile(ReferencedTabletFile.of(new Path("file:///accumulo/tables/2/t-1/f2.rf")),
            fateIds[3])
        .build(OPID, SELECTED);
    var tm5 = TabletMetadata.builder(extents[4])
        .putBulkFile(ReferencedTabletFile.of(new Path("file:///accumulo/tables/3/t-1/f3.rf")),
            fateIds[4])
        .putOperation(opid3).build(SELECTED);
    var tm6 = TabletMetadata.builder(extents[5]).putSelectedFiles(sf1).build(OPID, LOADED);
    var tm7 = TabletMetadata.builder(extents[6]).putSelectedFiles(sf2).build(OPID, LOADED);

    var tablets1 = Map.of(tm1.getExtent(), tm1, tm2.getExtent(), tm2, tm3.getExtent(), tm3,
        tm4.getExtent(), tm4, tm5.getExtent(), tm5, tm6.getExtent(), tm6, tm7.getExtent(), tm7);
    var tablets2 = new HashMap<>(tablets1);
    var found = new HashMap<KeyExtent,Set<FateId>>();
    Function<Collection<KeyExtent>,Map<KeyExtent,TabletMetadata>> tabletLookup = lookups -> {
      var results = new HashMap<KeyExtent,TabletMetadata>();
      lookups.forEach(extent -> {
        assertTrue(tablets1.containsKey(extent));
        if (tablets2.containsKey(extent)) {
          results.put(extent, tablets2.get(extent));
        }
      });
      return results;
    };

    // run test where every fate id is considered inactive
    Admin.findDanglingFateOperations(tablets1.values(), tabletLookup, fateId -> false, found::put,
        3);
    assertEquals(Map.of(tm1.getExtent(), Set.of(fateIds[0]), tm2.getExtent(), Set.of(fateIds[1]),
        tm3.getExtent(), Set.of(fateIds[2]), tm4.getExtent(), Set.of(fateIds[3]), tm5.getExtent(),
        Set.of(fateIds[4], fateIds[5]), tm6.getExtent(), Set.of(fateIds[6]), tm7.getExtent(),
        Set.of(fateIds[7])), found);

    // run test where some of the fate ids are active
    Set<FateId> active = Set.of(fateIds[0], fateIds[2], fateIds[4], fateIds[6]);
    found.clear();
    Admin.findDanglingFateOperations(tablets1.values(), tabletLookup, active::contains, found::put,
        3);
    assertEquals(Map.of(tm2.getExtent(), Set.of(fateIds[1]), tm4.getExtent(), Set.of(fateIds[3]),
        tm5.getExtent(), Set.of(fateIds[5]), tm7.getExtent(), Set.of(fateIds[7])), found);

    // run test where tablets change on 2nd read simulating race condition
    var tm2_1 = TabletMetadata.builder(tm2.getExtent()).build(OPID, LOADED, SELECTED);
    var tm4_1 = TabletMetadata.builder(tm4.getExtent())
        .putBulkFile(ReferencedTabletFile.of(new Path("file:///accumulo/tables/2/t-1/f2.rf")),
            fateIds[8])
        .build(OPID, SELECTED);
    tablets2.put(tm2_1.getExtent(), tm2_1);
    tablets2.put(tm4_1.getExtent(), tm4_1);
    tablets2.remove(tm5.getExtent());
    found.clear();
    Admin.findDanglingFateOperations(tablets1.values(), tabletLookup, active::contains, found::put,
        3);
    assertEquals(Map.of(tm7.getExtent(), Set.of(fateIds[7])), found);
    found.clear();

    // run a test where all are active on second look
    var tm7_1 = TabletMetadata.builder(tm7.getExtent()).putSelectedFiles(sf1).build(OPID, LOADED);
    tablets2.put(tm7_1.getExtent(), tm7_1);
    Admin.findDanglingFateOperations(tablets1.values(), tabletLookup, active::contains, found::put,
        3);
    assertEquals(Map.of(), found);

    // run a test where all active on the first look
    active = Arrays.stream(fateIds).collect(Collectors.toSet());
    found.clear();
    Admin.findDanglingFateOperations(tablets1.values(), le -> {
      assertTrue(le.isEmpty());
      return Map.of();
    }, active::contains, found::put, 3);
    assertEquals(Map.of(), found);
  }
}
