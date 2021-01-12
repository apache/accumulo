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
package org.apache.accumulo.server.master.state;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.server.metadata.TabletMutatorBase;
import org.junit.Test;

import com.google.common.base.Preconditions;

public class RootTabletStateStoreTest {

  private static class TestAmple implements Ample {

    private String json = new String(
        RootTabletMetadata.getInitialJson("dir", "hdfs://nn/acc/tables/some/dir/0000.rf"), UTF_8);

    @Override
    public TabletMetadata readTablet(KeyExtent extent, ReadConsistency rc,
        ColumnType... colsToFetch) {
      Preconditions.checkArgument(extent.equals(RootTable.EXTENT));
      return RootTabletMetadata.fromJson(json).convertToTabletMetadata();
    }

    @Override
    public TabletMutator mutateTablet(KeyExtent extent) {
      Preconditions.checkArgument(extent.equals(RootTable.EXTENT));
      return new TabletMutatorBase(null, extent) {

        @Override
        public void mutate() {
          Mutation m = getMutation();

          RootTabletMetadata rtm = RootTabletMetadata.fromJson(json);

          rtm.update(m);

          json = rtm.toJson();
        }
      };
    }

  }

  @Test
  public void testRootTabletStateStore() throws DistributedStoreException {
    ZooTabletStateStore tstore = new ZooTabletStateStore(new TestAmple());
    KeyExtent root = RootTable.EXTENT;
    String sessionId = "this is my unique session data";
    TServerInstance server =
        new TServerInstance(HostAndPort.fromParts("127.0.0.1", 10000), sessionId);
    List<Assignment> assignments = Collections.singletonList(new Assignment(root, server));
    tstore.setFutureLocations(assignments);
    int count = 0;
    for (TabletLocationState location : tstore) {
      assertEquals(location.extent, root);
      assertEquals(location.future, server);
      assertNull(location.current);
      count++;
    }
    assertEquals(count, 1);
    tstore.setLocations(assignments);
    count = 0;
    for (TabletLocationState location : tstore) {
      assertEquals(location.extent, root);
      assertNull(location.future);
      assertEquals(location.current, server);
      count++;
    }
    assertEquals(count, 1);
    TabletLocationState assigned = null;
    try {
      assigned = new TabletLocationState(root, server, null, null, null, null, false);
    } catch (BadLocationStateException e) {
      fail("Unexpected error " + e);
    }
    tstore.unassign(Collections.singletonList(assigned), null);
    count = 0;
    for (TabletLocationState location : tstore) {
      assertEquals(location.extent, root);
      assertNull(location.future);
      assertNull(location.current);
      count++;
    }
    assertEquals(count, 1);

    KeyExtent notRoot = new KeyExtent(TableId.of("0"), null, null);
    try {
      tstore.setLocations(Collections.singletonList(new Assignment(notRoot, server)));
      fail("should not get here");
    } catch (IllegalArgumentException ex) {}

    try {
      tstore.setFutureLocations(Collections.singletonList(new Assignment(notRoot, server)));
      fail("should not get here");
    } catch (IllegalArgumentException ex) {}

    TabletLocationState broken = null;
    try {
      broken = new TabletLocationState(notRoot, server, null, null, null, null, false);
    } catch (BadLocationStateException e) {
      fail("Unexpected error " + e);
    }
    try {
      tstore.unassign(Collections.singletonList(broken), null);
      fail("should not get here");
    } catch (IllegalArgumentException ex) {}
  }
}
