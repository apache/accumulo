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
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
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

  private static final InstanceId instance = InstanceId.of(UUID.randomUUID());
  private static final TableId tableId = TableId.of("testTable");
  private static final NamespaceId namespaceId = NamespaceId.of("testNamespace");

  private static class CompactionTestDriver extends CompactionDriver {
    private static final long serialVersionUID = 1L;
    private final boolean cancelled;

    static CompactionDriver cancelled() {
      return new CompactionTestDriver(true);
    }

    static CompactionDriver notCancelled() {
      return new CompactionTestDriver(false);
    }

    private CompactionTestDriver(boolean cancelled) {
      super(namespaceId, tableId, new byte[0], new byte[0]);
      this.cancelled = cancelled;
    }

    @Override
    protected boolean isCancelled(FateId fateId, ServerContext context) {
      return cancelled;
    }
  }

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
    runDriver(CompactionTestDriver.cancelled(), TableOperationsImpl.COMPACTION_CANCELED_MSG);
  }

  @Test
  public void testTableBeingDeleted() throws Exception {
    String deleteMarkerPath = PreDeleteTable.createDeleteMarkerPath(instance, tableId, namespaceId);
    expect(zk.exists(deleteMarkerPath, null)).andReturn(new Stat());
    runDriver(CompactionTestDriver.notCancelled(), TableOperationsImpl.TABLE_DELETED_MSG);
  }

  private void runDriver(CompactionDriver driver, String expectedMessage) {
    replay(manager, ctx, zk);
    var e = assertThrows(AcceptableThriftTableOperationException.class,
        () -> driver.isReady(FateId.from(FateInstanceType.USER, UUID.randomUUID()), manager));
    assertEquals(e.getTableId(), tableId.toString());
    assertEquals(e.getOp(), TableOperation.COMPACT);
    assertEquals(e.getType(), TableOperationExceptionType.OTHER);
    assertEquals(expectedMessage, e.getDescription());
  }
}
