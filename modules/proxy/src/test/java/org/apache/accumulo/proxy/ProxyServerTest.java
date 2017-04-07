/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.proxy;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.proxy.ProxyServer.BatchWriterPlusProblem;
import org.apache.accumulo.proxy.thrift.ColumnUpdate;
import org.apache.accumulo.proxy.thrift.WriterOptions;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ProxyServerTest {

  @Test
  public void updateAndFlushClosesWriterOnExceptionFromAddCells() throws Exception {
    ProxyServer server = EasyMock.createMockBuilder(ProxyServer.class).addMockedMethod("getWriter", ByteBuffer.class, String.class, WriterOptions.class)
        .addMockedMethod("addCellsToWriter", Map.class, BatchWriterPlusProblem.class).createMock();
    BatchWriter writer = EasyMock.createMock(BatchWriter.class);
    BatchWriterPlusProblem bwpe = new BatchWriterPlusProblem();
    bwpe.writer = writer;
    MutationsRejectedException mre = EasyMock.createMock(MutationsRejectedException.class);

    final ByteBuffer login = ByteBuffer.wrap("my_login".getBytes(UTF_8));
    final String tableName = "table1";
    final Map<ByteBuffer,List<ColumnUpdate>> cells = new HashMap<>();

    EasyMock.expect(server.getWriter(login, tableName, null)).andReturn(bwpe);
    server.addCellsToWriter(cells, bwpe);
    EasyMock.expectLastCall();

    // Set the exception
    bwpe.exception = mre;

    writer.close();
    EasyMock.expectLastCall();

    EasyMock.replay(server, writer, mre);

    try {
      server.updateAndFlush(login, tableName, cells);
      Assert.fail("Expected updateAndFlush to throw an exception");
    } catch (org.apache.accumulo.proxy.thrift.MutationsRejectedException e) {
      // pass
    }

    EasyMock.verify(server, writer, mre);
  }

  @Test
  public void updateAndFlushClosesWriterOnExceptionFromFlush() throws Exception {
    ProxyServer server = EasyMock.createMockBuilder(ProxyServer.class).addMockedMethod("getWriter", ByteBuffer.class, String.class, WriterOptions.class)
        .addMockedMethod("addCellsToWriter", Map.class, BatchWriterPlusProblem.class).createMock();
    BatchWriter writer = EasyMock.createMock(BatchWriter.class);
    BatchWriterPlusProblem bwpe = new BatchWriterPlusProblem();
    bwpe.writer = writer;
    MutationsRejectedException mre = EasyMock.createMock(MutationsRejectedException.class);

    final ByteBuffer login = ByteBuffer.wrap("my_login".getBytes(UTF_8));
    final String tableName = "table1";
    final Map<ByteBuffer,List<ColumnUpdate>> cells = new HashMap<>();

    EasyMock.expect(server.getWriter(login, tableName, null)).andReturn(bwpe);
    server.addCellsToWriter(cells, bwpe);
    EasyMock.expectLastCall();

    // No exception throw adding the cells
    bwpe.exception = null;

    writer.flush();
    EasyMock.expectLastCall().andThrow(mre);

    writer.close();
    EasyMock.expectLastCall();

    EasyMock.replay(server, writer, mre);

    try {
      server.updateAndFlush(login, tableName, cells);
      Assert.fail("Expected updateAndFlush to throw an exception");
    } catch (org.apache.accumulo.proxy.thrift.MutationsRejectedException e) {
      // pass
    }

    EasyMock.verify(server, writer, mre);
  }

}
