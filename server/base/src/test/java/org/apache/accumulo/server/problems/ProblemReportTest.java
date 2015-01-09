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
package org.apache.accumulo.server.problems;

import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.util.Encoding;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class ProblemReportTest {
  private static final String TABLE = "table";
  private static final String RESOURCE = "resource";
  private static final String SERVER = "server";

  private Instance instance;
  private ZooReaderWriter zoorw;
  private ProblemReport r;

  @Before
  public void setUp() throws Exception {
    instance = createMock(Instance.class);
    expect(instance.getInstanceID()).andReturn("instance");
    replay(instance);

    zoorw = createMock(ZooReaderWriter.class);
  }

  @Test
  public void testGetters() {
    long now = System.currentTimeMillis();
    r = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER, null, now);
    assertEquals(TABLE, r.getTableName());
    assertSame(ProblemType.FILE_READ, r.getProblemType());
    assertEquals(RESOURCE, r.getResource());
    assertEquals(SERVER, r.getServer());
    assertEquals(now, r.getTime());
    assertNull(r.getException());
  }

  @Test
  public void testWithException() {
    Exception e = new IllegalArgumentException("Oh noes");
    r = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER, e);
    assertEquals("Oh noes", r.getException());
  }

  @Test
  public void testEquals() {
    r = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    assertTrue(r.equals(r));
    ProblemReport r2 = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    assertTrue(r.equals(r2));
    assertTrue(r2.equals(r));
    ProblemReport rx1 = new ProblemReport(TABLE + "x", ProblemType.FILE_READ, RESOURCE, SERVER, null);
    assertFalse(r.equals(rx1));
    ProblemReport rx2 = new ProblemReport(TABLE, ProblemType.FILE_WRITE, RESOURCE, SERVER, null);
    assertFalse(r.equals(rx2));
    ProblemReport rx3 = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE + "x", SERVER, null);
    assertFalse(r.equals(rx3));
    ProblemReport re1 = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER + "x", null);
    assertTrue(r.equals(re1));
    ProblemReport re2 = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER, new IllegalArgumentException("yikes"));
    assertTrue(r.equals(re2));
  }

  @Test
  public void testEqualsNull() {
    r = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    assertFalse(r.equals(null));
  }

  @Test
  public void testHashCode() {
    r = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    ProblemReport r2 = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    assertEquals(r.hashCode(), r2.hashCode());
    ProblemReport re1 = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER + "x", null);
    assertEquals(r.hashCode(), re1.hashCode());
    ProblemReport re2 = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER, new IllegalArgumentException("yikes"));
    assertEquals(r.hashCode(), re2.hashCode());
  }

  private byte[] makeZPathFileName(String table, ProblemType problemType, String resource) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeUTF(table);
    dos.writeUTF(problemType.name());
    dos.writeUTF(resource);
    dos.close();
    return baos.toByteArray();
  }

  private byte[] encodeReportData(long creationTime, String server, String exception) throws Exception {
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
    r = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER, null);
    byte[] zpathFileName = makeZPathFileName(TABLE, ProblemType.FILE_READ, RESOURCE);
    String path = ZooUtil.getRoot("instance") + Constants.ZPROBLEMS + "/" + Encoding.encodeAsBase64FileName(new Text(zpathFileName));
    zoorw.recursiveDelete(path, NodeMissingPolicy.SKIP);
    replay(zoorw);

    r.removeFromZooKeeper(zoorw, instance);
    verify(zoorw);
  }

  @Test
  public void testSaveToZooKeeper() throws Exception {
    long now = System.currentTimeMillis();
    r = new ProblemReport(TABLE, ProblemType.FILE_READ, RESOURCE, SERVER, null, now);
    byte[] zpathFileName = makeZPathFileName(TABLE, ProblemType.FILE_READ, RESOURCE);
    String path = ZooUtil.getRoot("instance") + Constants.ZPROBLEMS + "/" + Encoding.encodeAsBase64FileName(new Text(zpathFileName));
    byte[] encoded = encodeReportData(now, SERVER, null);
    expect(zoorw.putPersistentData(eq(path), aryEq(encoded), eq(NodeExistsPolicy.OVERWRITE))).andReturn(true);
    replay(zoorw);

    r.saveToZooKeeper(zoorw, instance);
    verify(zoorw);
  }

  @Test
  public void testDecodeZooKeeperEntry() throws Exception {
    byte[] zpathFileName = makeZPathFileName(TABLE, ProblemType.FILE_READ, RESOURCE);
    String node = Encoding.encodeAsBase64FileName(new Text(zpathFileName));
    long now = System.currentTimeMillis();
    byte[] encoded = encodeReportData(now, SERVER, "excmsg");

    expect(zoorw.getData(ZooUtil.getRoot("instance") + Constants.ZPROBLEMS + "/" + node, null)).andReturn(encoded);
    replay(zoorw);

    r = ProblemReport.decodeZooKeeperEntry(node, zoorw, instance);
    assertEquals(TABLE, r.getTableName());
    assertSame(ProblemType.FILE_READ, r.getProblemType());
    assertEquals(RESOURCE, r.getResource());
    assertEquals(SERVER, r.getServer());
    assertEquals(now, r.getTime());
    assertEquals("excmsg", r.getException());
  }
}
