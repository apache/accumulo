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
package org.apache.accumulo.server.problems;

import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.util.Encoding;
import org.apache.accumulo.server.MockServerContext;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProblemReportTest {
  private static final TableId TABLE_ID = TableId.of("table");
  private static final String RESOURCE = "resource";
  private static final String SERVER = "server";

  private ServerContext context;
  private ZooReaderWriter zoorw;
  private ProblemReport r;

  @BeforeEach
  public void setUp() {
    context = MockServerContext.getWithZK(InstanceId.of("instance"), "", 30_000);
    zoorw = createMock(ZooReaderWriter.class);
    expect(context.getZooReaderWriter()).andReturn(zoorw).anyTimes();
    replay(context);
  }

  @Test
  public void testGetters() {
    long now = System.currentTimeMillis();
    r = new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER, null, now);
    assertEquals(TABLE_ID, r.getTableId());
    assertSame(ProblemType.FILE_READ, r.getProblemType());
    assertEquals(RESOURCE, r.getResource());
    assertEquals(SERVER, r.getServer());
    assertEquals(now, r.getTime());
    assertNull(r.getException());
  }

  @Test
  public void testWithException() {
    Exception e = new IllegalArgumentException("Oh noes");
    r = new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER, e);
    assertEquals("Oh noes", r.getException());
  }

  @Test
  public void testEquals() {
    r = new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    assertEquals(r, r);
    ProblemReport r2 = new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    assertEquals(r, r2);
    assertEquals(r2, r);
    ProblemReport rx1 =
        new ProblemReport(MetadataTable.ID, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    assertNotEquals(r, rx1);
    ProblemReport rx2 = new ProblemReport(TABLE_ID, ProblemType.FILE_WRITE, RESOURCE, SERVER, null);
    assertNotEquals(r, rx2);
    ProblemReport rx3 =
        new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE + "x", SERVER, null);
    assertNotEquals(r, rx3);
    ProblemReport re1 =
        new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER + "x", null);
    assertEquals(r, re1);
    ProblemReport re2 = new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER,
        new IllegalArgumentException("yikes"));
    assertEquals(r, re2);
  }

  @Test
  public void testEqualsNull() {
    r = new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    assertFalse(r.equals(null));
  }

  @Test
  public void testHashCode() {
    r = new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    ProblemReport r2 = new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    assertEquals(r.hashCode(), r2.hashCode());
    ProblemReport re1 =
        new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER + "x", null);
    assertEquals(r.hashCode(), re1.hashCode());
    ProblemReport re2 = new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER,
        new IllegalArgumentException("yikes"));
    assertEquals(r.hashCode(), re2.hashCode());
  }

  private byte[] makeZPathFileName(TableId table, ProblemType problemType, String resource)
      throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeUTF(table.canonical());
    dos.writeUTF(problemType.name());
    dos.writeUTF(resource);
    dos.close();
    return baos.toByteArray();
  }

  private byte[] encodeReportData(long creationTime, String server, String exception)
      throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeLong(creationTime);
    dos.writeBoolean(server != null);
    if (server != null) {
      dos.writeUTF(server);
    }
    dos.writeBoolean(exception != null);
    if (exception != null) {
      dos.writeUTF(exception);
    }
    dos.close();
    return baos.toByteArray();
  }

  @Test
  public void testRemoveFromZooKeeper() throws Exception {
    r = new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    byte[] zpathFileName = makeZPathFileName(TABLE_ID, ProblemType.FILE_READ, RESOURCE);
    String path = ZooUtil.getRoot(InstanceId.of("instance")) + Constants.ZPROBLEMS + "/"
        + Encoding.encodeAsBase64FileName(new Text(zpathFileName));
    zoorw.recursiveDelete(path, NodeMissingPolicy.SKIP);
    replay(zoorw);

    r.removeFromZooKeeper(zoorw, context);
    verify(zoorw);
  }

  @Test
  public void testSaveToZooKeeper() throws Exception {
    long now = System.currentTimeMillis();
    r = new ProblemReport(TABLE_ID, ProblemType.FILE_READ, RESOURCE, SERVER, null, now);
    byte[] zpathFileName = makeZPathFileName(TABLE_ID, ProblemType.FILE_READ, RESOURCE);
    String path = ZooUtil.getRoot(InstanceId.of("instance")) + Constants.ZPROBLEMS + "/"
        + Encoding.encodeAsBase64FileName(new Text(zpathFileName));
    byte[] encoded = encodeReportData(now, SERVER, null);
    expect(zoorw.putPersistentData(eq(path), aryEq(encoded), eq(NodeExistsPolicy.OVERWRITE)))
        .andReturn(true);
    replay(zoorw);

    r.saveToZooKeeper(context);
    verify(zoorw);
  }

  @Test
  public void testDecodeZooKeeperEntry() throws Exception {
    byte[] zpathFileName = makeZPathFileName(TABLE_ID, ProblemType.FILE_READ, RESOURCE);
    String node = Encoding.encodeAsBase64FileName(new Text(zpathFileName));
    long now = System.currentTimeMillis();
    byte[] encoded = encodeReportData(now, SERVER, "excmsg");

    expect(zoorw
        .getData(ZooUtil.getRoot(InstanceId.of("instance")) + Constants.ZPROBLEMS + "/" + node))
        .andReturn(encoded);
    replay(zoorw);

    r = ProblemReport.decodeZooKeeperEntry(context, node);
    assertEquals(TABLE_ID, r.getTableId());
    assertSame(ProblemType.FILE_READ, r.getProblemType());
    assertEquals(RESOURCE, r.getResource());
    assertEquals(SERVER, r.getServer());
    assertEquals(now, r.getTime());
    assertEquals("excmsg", r.getException());
  }
}
