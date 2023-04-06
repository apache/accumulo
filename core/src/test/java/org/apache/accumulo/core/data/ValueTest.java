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
package org.apache.accumulo.core.data;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ValueTest {
  private static final byte[] toBytes(String s) {
    return s.getBytes(UTF_8);
  }

  private static final byte[] DATA = toBytes("data");
  private static final ByteBuffer DATABUFF = ByteBuffer.allocate(DATA.length);
  static {
    DATABUFF.put(DATA);
  }

  @BeforeEach
  public void setUp() {
    DATABUFF.rewind();
  }

  @Test
  public void testDefault() {
    Value v = new Value();
    assertEquals(0, v.get().length);
  }

  @Test
  public void testNullBytesConstructor() {
    assertThrows(NullPointerException.class, () -> new Value((byte[]) null));
  }

  @Test
  public void testNullCopyConstructor() {
    assertThrows(NullPointerException.class, () -> new Value((Value) null));
  }

  @Test
  public void testNullByteBufferConstructor() {
    assertThrows(NullPointerException.class, () -> new Value((ByteBuffer) null));
  }

  @Test
  public void testNullSet() {
    Value v = new Value();
    assertThrows(NullPointerException.class, () -> v.set(null));
  }

  @Test
  public void testByteArray() {
    Value v = new Value(DATA);
    assertArrayEquals(DATA, v.get());
    assertSame(DATA, v.get());
  }

  @Test
  public void testByteArrayCopy() {
    Value v = new Value(DATA, true);
    assertArrayEquals(DATA, v.get());
    assertNotSame(DATA, v.get());
  }

  @Test
  public void testByteBuffer() {
    Value v = new Value(DATABUFF);
    assertArrayEquals(DATA, v.get());
  }

  @Test
  public void testValueCopy() {
    Value ov = createMock(Value.class);
    expect(ov.get()).andReturn(DATA);
    expect(ov.getSize()).andReturn(4);
    replay(ov);
    Value v = new Value(ov);
    assertArrayEquals(DATA, v.get());
  }

  @Test
  public void testByteArrayOffsetLength() {
    Value v = new Value(DATA, 0, 4);
    assertArrayEquals(DATA, v.get());
  }

  @Test
  public void testSet() {
    Value v = new Value();
    v.set(DATA);
    assertArrayEquals(DATA, v.get());
    assertSame(DATA, v.get());
  }

  @Test
  public void testCopy() {
    Value v = new Value();
    v.copy(DATA);
    assertArrayEquals(DATA, v.get());
    assertNotSame(DATA, v.get());
  }

  @Test
  public void testGetSize() {
    Value v = new Value(DATA);
    assertEquals(DATA.length, v.getSize());
  }

  @Test
  public void testGetSizeDefault() {
    Value v = new Value();
    assertEquals(0, v.getSize());
  }

  @Test
  public void testWriteRead() throws Exception {
    Value v = new Value(DATA);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    v.write(dos);
    dos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    Value v2 = new Value();
    v2.readFields(dis);
    dis.close();
    assertArrayEquals(DATA, v2.get());
  }

  @Test
  public void testHashCode() {
    Value v1 = new Value(DATA);
    Value v2 = new Value(DATA);
    assertEquals(v1.hashCode(), v2.hashCode());
  }

  @Test
  public void testCompareTo() {
    Value v1 = new Value(DATA);
    Value v2 = new Value(toBytes("datb"));
    assertTrue(v1.compareTo(v2) < 0);
    assertTrue(v2.compareTo(v1) > 0);
    Value v1a = new Value(DATA);
    assertEquals(0, v1.compareTo(v1a));
    Value v3 = new Value(toBytes("datc"));
    assertTrue(v2.compareTo(v3) < 0);
    assertTrue(v1.compareTo(v3) < 0);
  }

  @Test
  public void testEquals() {
    Value v1 = new Value(DATA);
    assertEquals(v1, v1);
    Value v2 = new Value(DATA);
    assertEquals(v1, v2);
    assertEquals(v2, v1);
    Value v3 = new Value(toBytes("datb"));
    assertNotEquals(v1, v3);
  }

  @Test
  public void testString() {
    Value v1 = new Value("abc");
    Value v2 = new Value("abc".getBytes(UTF_8));
    assertEquals(v2, v1);
  }

  @Test
  public void testNullCharSequence() {
    assertThrows(NullPointerException.class, () -> new Value((CharSequence) null));
  }

  @Test
  public void testText() {
    Value v1 = new Value(new Text("abc"));
    Value v2 = new Value("abc");
    assertEquals(v2, v1);
  }

  @Test
  public void testNullText() {
    assertThrows(NullPointerException.class, () -> new Value((Text) null));
  }
}
