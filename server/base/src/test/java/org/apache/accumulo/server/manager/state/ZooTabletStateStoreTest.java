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
package org.apache.accumulo.server.manager.state;

import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.MockServerContext;
import org.apache.accumulo.server.ServerContext;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ZooTabletStateStoreTest {

  @Test
  public void testZooTabletStateStore() throws DistributedStoreException {

    ServiceLock lock = EasyMock.createMock(ServiceLock.class);
    ServerContext context = MockServerContext.get();
    Ample ample = EasyMock.createMock(Ample.class);
    expect(context.getAmple()).andReturn(ample).anyTimes();
    expect(context.getServiceLock()).andReturn(lock).anyTimes();
    EasyMock.replay(lock, context, ample);
    ZooTabletStateStore tstore = new ZooTabletStateStore(DataLevel.ROOT, context);

    String sessionId = "this is my unique session data";
    TServerInstance server =
        new TServerInstance(HostAndPort.fromParts("127.0.0.1", 10000), sessionId);

    KeyExtent notRoot = new KeyExtent(TableId.of("0"), null, null);
    final var assignmentList = List.of(new Assignment(notRoot, server, null));
    assertThrows(IllegalArgumentException.class, () -> tstore.setLocations(assignmentList));
    assertThrows(IllegalArgumentException.class, () -> tstore.setFutureLocations(assignmentList));

    var nonRootMeta = TabletMetadata.builder(new KeyExtent(TableId.of("notroot"), null, null))
        .putPrevEndRow(null).build();

    final List<TabletMetadata> assignmentList1 = List.of(nonRootMeta);
    assertThrows(IllegalArgumentException.class, () -> tstore.unassign(assignmentList1, null));

    EasyMock.verify(lock, context, ample);

  }
}
