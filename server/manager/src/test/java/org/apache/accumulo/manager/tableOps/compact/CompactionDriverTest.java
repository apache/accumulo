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
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.delete.PreDeleteTable;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class CompactionDriverTest {

  public static class CancelledCompactionDriver extends CompactionDriver {

    private static final long serialVersionUID = 1L;

    public CancelledCompactionDriver(NamespaceId namespaceId, TableId tableId, byte[] startRow,
        byte[] endRow) {
      super(namespaceId, tableId, startRow, endRow);
    }

    @Override
    protected boolean isCancelled(FateId fateId, ServerContext context)
        throws InterruptedException, KeeperException {
      return true;
    }

  }

  public static class NotCancelledCompactionDriver extends CompactionDriver {

    private static final long serialVersionUID = 1L;

    public NotCancelledCompactionDriver(NamespaceId namespaceId, TableId tableId, byte[] startRow,
        byte[] endRow) {
      super(namespaceId, tableId, startRow, endRow);
    }

    @Override
    protected boolean isCancelled(FateId fateId, ServerContext context)
        throws InterruptedException, KeeperException {
      return false;
    }

  }

  @Test
  public void testCancelId() throws Exception {

    final InstanceId instance = InstanceId.of(UUID.randomUUID());
    final NamespaceId namespaceId = NamespaceId.of("13");
    final TableId tableId = TableId.of("42");
    final FateId compactionFateId = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    final byte[] startRow = new byte[0];
    final byte[] endRow = new byte[0];

    Manager manager = EasyMock.createNiceMock(Manager.class);
    ServerContext ctx = EasyMock.createNiceMock(ServerContext.class);
    ZooReaderWriter zrw = EasyMock.createNiceMock(ZooReaderWriter.class);
    EasyMock.expect(manager.getInstanceID()).andReturn(instance).anyTimes();
    EasyMock.expect(manager.getContext()).andReturn(ctx);
    EasyMock.expect(ctx.getZooReaderWriter()).andReturn(zrw);

    EasyMock.replay(manager, ctx, zrw);

    final CancelledCompactionDriver driver =
        new CancelledCompactionDriver(namespaceId, tableId, startRow, endRow);

    var e = assertThrows(AcceptableThriftTableOperationException.class,
        () -> driver.isReady(compactionFateId, manager));

    assertEquals(e.getTableId(), tableId.toString());
    assertEquals(e.getOp(), TableOperation.COMPACT);
    assertEquals(e.getType(), TableOperationExceptionType.OTHER);
    assertEquals(TableOperationsImpl.COMPACTION_CANCELED_MSG, e.getDescription());

    EasyMock.verify(manager, ctx, zrw);
  }

  @Test
  public void testTableBeingDeleted() throws Exception {

    final InstanceId instance = InstanceId.of(UUID.randomUUID());
    final NamespaceId namespaceId = NamespaceId.of("14");
    final TableId tableId = TableId.of("43");
    final FateId compactionFateId = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    final byte[] startRow = new byte[0];
    final byte[] endRow = new byte[0];

    Manager manager = EasyMock.createNiceMock(Manager.class);
    ServerContext ctx = EasyMock.createNiceMock(ServerContext.class);
    ZooReaderWriter zrw = EasyMock.createNiceMock(ZooReaderWriter.class);
    EasyMock.expect(manager.getInstanceID()).andReturn(instance).anyTimes();
    EasyMock.expect(manager.getContext()).andReturn(ctx);
    EasyMock.expect(ctx.getZooReaderWriter()).andReturn(zrw);

    String deleteMarkerPath = PreDeleteTable.createDeleteMarkerPath(instance, tableId);
    EasyMock.expect(zrw.exists(deleteMarkerPath)).andReturn(true);

    EasyMock.replay(manager, ctx, zrw);

    final NotCancelledCompactionDriver driver =
        new NotCancelledCompactionDriver(namespaceId, tableId, startRow, endRow);

    var e = assertThrows(AcceptableThriftTableOperationException.class,
        () -> driver.isReady(compactionFateId, manager));

    assertEquals(e.getTableId(), tableId.toString());
    assertEquals(e.getOp(), TableOperation.COMPACT);
    assertEquals(e.getType(), TableOperationExceptionType.OTHER);
    assertEquals(TableOperationsImpl.TABLE_DELETED_MSG, e.getDescription());

    EasyMock.verify(manager, ctx, zrw);
  }

}
