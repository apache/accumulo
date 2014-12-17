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
package org.apache.accumulo.core.security.crypto;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class BlockedIOStreamTest {
  @Test
  public void testLargeBlockIO() throws IOException {
    writeRead(1024, 2048);
  }

  private void writeRead(int blockSize, int expectedSize) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BlockedOutputStream blockOut = new BlockedOutputStream(baos, blockSize, 1);

    String contentString = "My Blocked Content String";
    byte[] content = contentString.getBytes(UTF_8);
    blockOut.write(content);
    blockOut.flush();

    String contentString2 = "My Other Blocked Content String";
    byte[] content2 = contentString2.getBytes(UTF_8);
    blockOut.write(content2);
    blockOut.flush();

    blockOut.close();
    byte[] written = baos.toByteArray();
    assertEquals(expectedSize, written.length);

    ByteArrayInputStream biis = new ByteArrayInputStream(written);
    BlockedInputStream blockIn = new BlockedInputStream(biis, blockSize, blockSize);
    DataInputStream dIn = new DataInputStream(blockIn);

    dIn.readFully(content, 0, content.length);
    String readContentString = new String(content, UTF_8);

    assertEquals(contentString, readContentString);

    dIn.readFully(content2, 0, content2.length);
    String readContentString2 = new String(content2, UTF_8);

    assertEquals(contentString2, readContentString2);

    blockIn.close();
  }

  @Test
  public void testSmallBufferBlockedIO() throws IOException {
    writeRead(16, (12 + 4) * (int) (Math.ceil(25.0 / 12) + Math.ceil(31.0 / 12)));
  }

  @Test
  public void testSpillingOverOutputStream() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // buffer will be size 12
    BlockedOutputStream blockOut = new BlockedOutputStream(baos, 16, 16);
    Random r = new Random(22);

    byte[] undersized = new byte[11];
    byte[] perfectSized = new byte[12];
    byte[] overSized = new byte[13];
    byte[] perfectlyOversized = new byte[13];
    byte filler = (byte) r.nextInt();

    r.nextBytes(undersized);
    r.nextBytes(perfectSized);
    r.nextBytes(overSized);
    r.nextBytes(perfectlyOversized);

    // 1 block
    blockOut.write(undersized);
    blockOut.write(filler);
    blockOut.flush();

    // 2 blocks
    blockOut.write(perfectSized);
    blockOut.write(filler);
    blockOut.flush();

    // 2 blocks
    blockOut.write(overSized);
    blockOut.write(filler);
    blockOut.flush();

    // 3 blocks
    blockOut.write(undersized);
    blockOut.write(perfectlyOversized);
    blockOut.write(filler);
    blockOut.flush();

    blockOut.close();
    assertEquals(16 * 8, baos.toByteArray().length);
  }

  @Test
  public void testGiantWrite() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int blockSize = 16;
    // buffer will be size 12
    BlockedOutputStream blockOut = new BlockedOutputStream(baos, blockSize, blockSize);
    Random r = new Random(22);

    int size = 1024 * 1024 * 128;
    byte[] giant = new byte[size];
    byte[] pattern = new byte[1024];

    r.nextBytes(pattern);

    for (int i = 0; i < size / 1024; i++) {
      System.arraycopy(pattern, 0, giant, i * 1024, 1024);
    }

    blockOut.write(giant);
    blockOut.flush();

    blockOut.close();
    baos.close();

    int blocks = (int) Math.ceil(size / (blockSize - 4.0));
    byte[] byteStream = baos.toByteArray();

    assertEquals(blocks * 16, byteStream.length);

    DataInputStream blockIn = new DataInputStream(new BlockedInputStream(new ByteArrayInputStream(byteStream), blockSize, blockSize));
    Arrays.fill(giant, (byte) 0);
    blockIn.readFully(giant, 0, size);
    blockIn.close();

    for (int i = 0; i < size / 1024; i++) {
      byte[] readChunk = new byte[1024];
      System.arraycopy(giant, i * 1024, readChunk, 0, 1024);
      assertArrayEquals(pattern, readChunk);
    }
  }

}
