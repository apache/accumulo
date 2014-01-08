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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.accumulo.core.Constants;
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
    byte[] content = contentString.getBytes(Constants.UTF8);
    blockOut.write(content);
    blockOut.flush();

    String contentString2 = "My Other Blocked Content String";
    byte[] content2 = contentString2.getBytes(Constants.UTF8);
    blockOut.write(content2);
    blockOut.flush();

    blockOut.close();
    byte[] written = baos.toByteArray();
    assertEquals(expectedSize, written.length);

    ByteArrayInputStream biis = new ByteArrayInputStream(written);
    BlockedInputStream blockIn = new BlockedInputStream(biis, blockSize, blockSize);
    DataInputStream dIn = new DataInputStream(blockIn);

    dIn.readFully(content, 0, content.length);
    String readContentString = new String(content, Constants.UTF8);

    assertEquals(contentString, readContentString);

    dIn.readFully(content2, 0, content2.length);
    String readContentString2 = new String(content2, Constants.UTF8);

    assertEquals(contentString2, readContentString2);

    blockIn.close();
  }

  @Test
  public void testSmallBufferBlockedIO() throws IOException {
    writeRead(16, (12 + 4) * (int) (Math.ceil(25.0/12) + Math.ceil(31.0/12)));
  }
}
