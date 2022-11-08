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
package org.apache.accumulo.core.client.security.tokens;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.List;

import javax.security.auth.DestroyFailedException;

import org.junit.jupiter.api.Test;

public class PasswordTokenTest {

  @Test
  public void testMultiByte() throws DestroyFailedException {
    PasswordToken pt = new PasswordToken();
    AuthenticationToken.Properties props = new AuthenticationToken.Properties();
    props.put("password", "五六");
    pt.init(props);
    props.destroy();
    String s = new String(pt.getPassword(), UTF_8);
    assertEquals("五六", s);

    pt = new PasswordToken("五六");
    s = new String(pt.getPassword(), UTF_8);
    assertEquals("五六", s);
  }

  @Test
  public void testReadingLegacyFormat() throws IOException {
    String newFormat = "/////gAAAAh0ZXN0cGFzcw=="; // the new format without using GZip
    String oldFormat1 = "AAAAHB+LCAAAAAAAAAArSS0uKUgsLgYAGRFm+ggAAAA="; // jdk 11 GZip produced this
    String oldFormat2 = "AAAAHB+LCAAAAAAAAP8rSS0uKUgsLgYAGRFm+ggAAAA="; // jdk 17 GZip produced this
    for (String format : List.of(oldFormat1, oldFormat2, newFormat)) {
      byte[] array = Base64.getDecoder().decode(format);
      try (var bais = new ByteArrayInputStream(array); var dis = new DataInputStream(bais)) {
        var deserializedToken = new PasswordToken();
        deserializedToken.readFields(dis);
        assertArrayEquals("testpass".getBytes(UTF_8), deserializedToken.getPassword());
      }
    }
  }

  @Test
  public void testReadingAndWriting() throws IOException {
    var originalToken = new PasswordToken("testpass");
    byte[] saved;
    try (var baos = new ByteArrayOutputStream(); var dos = new DataOutputStream(baos)) {
      originalToken.write(dos);
      saved = baos.toByteArray();
    }
    try (var bais = new ByteArrayInputStream(saved); var dis = new DataInputStream(bais)) {
      var deserializedToken = new PasswordToken();
      deserializedToken.readFields(dis);
      assertArrayEquals(originalToken.getPassword(), deserializedToken.getPassword());
    }
  }
}
