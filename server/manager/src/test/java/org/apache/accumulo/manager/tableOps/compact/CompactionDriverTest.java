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
package org.apache.accumulo.manager.tableOps.compact;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.delete.PreDeleteTable;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CompactionDriverTest {

  private final InstanceId instance = InstanceId.of(UUID.randomUUID());
  private final NamespaceId namespaceId = NamespaceId.of("testNamespace");
  private final TableId tableId = TableId.of("testTable");
  private final byte[] startRow = new byte[0];
  private final byte[] endRow = new byte[0];
  private final long compactId = 123; // arbitrary

  private Manager manager;
  private ServerContext ctx;
  private ZooSession zk;

  @BeforeEach
  public void setup() {
    manager = createMock(Manager.class);
    ctx = createMock(ServerContext.class);
    zk = createMock(ZooSession.class);
    expect(ctx.getInstanceID()).andReturn(instance).anyTimes();
    expect(ctx.getZooSession()).andReturn(zk).anyTimes();
    expect(zk.asReaderWriter()).andReturn(new ZooReaderWriter(zk)).anyTimes();
    expect(manager.getContext()).andReturn(ctx).anyTimes();
  }

  @AfterEach
  public void teardown() {
    verify(manager, ctx, zk);
  }

  @Test
  public void testCancelId() throws Exception {
    runDriver(compactId + 1, TableOperationsImpl.COMPACTION_CANCELED_MSG);
  }

  @Test
  public void testTableBeingDeleted() throws Exception {
    String deleteMarkerPath = PreDeleteTable.createDeleteMarkerPath(instance, tableId);
    expect(zk.exists(deleteMarkerPath, null)).andReturn(new Stat()).once();
    runDriver(compactId - 1, TableOperationsImpl.TABLE_DELETED_MSG);
  }

  private void runDriver(long cancelId, String expectedMessage) throws Exception {
    final String zCancelID = CompactionDriver.createCompactionCancellationPath(instance, tableId);
    expect(zk.getData(zCancelID, null, null)).andReturn(Long.toString(cancelId).getBytes(UTF_8));

    final var driver = new CompactionDriver(compactId, namespaceId, tableId, startRow, endRow);
    final long mockTxId = tableId.hashCode();

    replay(manager, ctx, zk);
    var e = assertThrows(AcceptableThriftTableOperationException.class,
        () -> driver.isReady(mockTxId, manager));
    assertEquals(e.getTableId(), tableId.toString());
    assertEquals(e.getOp(), TableOperation.COMPACT);
    assertEquals(e.getType(), TableOperationExceptionType.OTHER);
    assertEquals(expectedMessage, e.getDescription());
  }

}
