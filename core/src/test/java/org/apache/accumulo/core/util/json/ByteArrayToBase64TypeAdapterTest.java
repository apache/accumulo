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
package org.apache.accumulo.core.util.json;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

public class ByteArrayToBase64TypeAdapterTest {

  private static final Gson gson = ByteArrayToBase64TypeAdapter.createBase64Gson();

  @Test
  public void testSerializeText() throws IOException {
    final Text original = new Text("This is a test");

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      original.write(new DataOutputStream(dos));

      final String encoded = gson.toJson(baos.toByteArray());
      final Text decoded = new Text();
      decoded.readFields(
          new DataInputStream(new ByteArrayInputStream(gson.fromJson(encoded, byte[].class))));
      assertEquals(original.toString(), decoded.toString());
    }
  }

  @Test
  public void testByteArray() {
    final byte[] original = new byte[] {0x01, 0x06, 0x34, 0x09, 0x12, 0x34, 0x57, 0x56, 0x30, 0x74};

    final String encoded = gson.toJson(original);
    final byte[] decoded = gson.fromJson(encoded, byte[].class);

    assertArrayEquals(original, decoded);
  }
}
