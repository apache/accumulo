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

package org.apache.accumulo.core.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class ByteBufferUtilTest {

  private static void assertEquals(String expected, ByteBuffer bb) {
    Assert.assertEquals(new Text(expected), ByteBufferUtil.toText(bb));
    Assert.assertEquals(expected, new String(ByteBufferUtil.toBytes(bb), UTF_8));
    Assert.assertEquals(expected, ByteBufferUtil.toString(bb));

    List<byte[]> bal = ByteBufferUtil.toBytesList(Collections.singletonList(bb));
    Assert.assertEquals(1, bal.size());
    Assert.assertEquals(expected, new String(bal.get(0), UTF_8));

    Assert.assertEquals(new ArrayByteSequence(expected), new ArrayByteSequence(bb));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      ByteBufferUtil.write(dos, bb);
      dos.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Assert.assertEquals(expected, new String(baos.toByteArray(), UTF_8));

    ByteArrayInputStream bais = ByteBufferUtil.toByteArrayInputStream(bb);
    byte[] buffer = new byte[expected.length()];
    try {
      bais.read(buffer);
      Assert.assertEquals(expected, new String(buffer, UTF_8));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testNonZeroArrayOffset() {
    byte[] data = "0123456789".getBytes(UTF_8);

    ByteBuffer bb1 = ByteBuffer.wrap(data, 3, 4);

    // create a ByteBuffer with a non-zero array offset
    ByteBuffer bb2 = bb1.slice();

    // The purpose of this test is to ensure ByteBufferUtil code works when arrayOffset is non-zero. The following asserts are not to test ByteBuffer, but
    // ensure the behavior of slice() is as expected.

    Assert.assertEquals(3, bb2.arrayOffset());
    Assert.assertEquals(0, bb2.position());
    Assert.assertEquals(4, bb2.limit());

    // start test with non zero arrayOffset
    assertEquals("3456", bb2);

    // read one byte from byte buffer... this should cause position to be non-zero in addition to array offset
    bb2.get();
    assertEquals("456", bb2);

  }

  @Test
  public void testZeroArrayOffsetAndNonZeroPosition() {
    byte[] data = "0123456789".getBytes(UTF_8);
    ByteBuffer bb1 = ByteBuffer.wrap(data, 3, 4);

    assertEquals("3456", bb1);
  }

  @Test
  public void testZeroArrayOffsetAndPosition() {
    byte[] data = "0123456789".getBytes(UTF_8);
    ByteBuffer bb1 = ByteBuffer.wrap(data, 0, 4);
    assertEquals("0123", bb1);
  }

  @Test
  public void testDirectByteBuffer() {
    // allocate direct so it does not have a backing array
    ByteBuffer bb = ByteBuffer.allocateDirect(10);
    bb.put("0123456789".getBytes(UTF_8));
    bb.rewind();

    assertEquals("0123456789", bb);

    // advance byte buffer position
    bb.get();
    assertEquals("123456789", bb);
  }
}
