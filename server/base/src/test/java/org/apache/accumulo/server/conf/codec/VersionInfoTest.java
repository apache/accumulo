/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.conf.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;

import org.junit.Test;

public class VersionInfoTest {

  @Test
  public void getInitialDataVersion() {
    VersionInfo info = new VersionInfo.Builder().build();
    assertEquals(0, info.getDataVersion());

    // the initial version for write should be 0
    assertEquals("Initial expected version should be 0", 0, info.getNextVersion());
  }

  @Test
  public void getTimestamp() {
    VersionInfo info = new VersionInfo.Builder().build();
    assertTrue("timestamp should be now or earlier",
        info.getTimestamp().compareTo(Instant.now()) <= 0);
  }

  @Test
  public void encodeRoundTrip() throws IOException {

    int aDataVersion = 37;
    Instant now = Instant.now();
    VersionInfo info =
        new VersionInfo.Builder().withDataVersion(aDataVersion).withTimestamp(now).build();

    byte[] encoded;

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {

      info.encode(dos);
      dos.flush();
      encoded = bos.toByteArray();
    }

    try (ByteArrayInputStream bis = new ByteArrayInputStream(encoded);
        DataInputStream dis = new DataInputStream(bis)) {

      VersionInfo decoded = new VersionInfo(dis);

      assertEquals(aDataVersion + 1, decoded.getDataVersion());

      // time in serialized version should be after test start time.
      assertTrue(now.compareTo(decoded.getTimestamp()) <= 0);
    }
  }

  @Test
  public void prettyTest() {
    VersionInfo info = new VersionInfo.Builder().build();
    assertFalse(info.toString().contains("\n"));
    assertTrue(info.print(true).contains("\n"));
  }
}
