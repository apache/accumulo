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
package org.apache.accumulo.core.clientImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;

import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TabletMergeabilityUtilTest {

  @Test
  public void testEncodeDecode() {
    var text = getRandomText();
    String json = TabletMergeabilityUtil.encode(text, TabletMergeability.never());
    var decoded = TabletMergeabilityUtil.decode(json);
    assertEquals(text, decoded.getFirst());
    assertEquals(TabletMergeability.never(), decoded.getSecond());
  }

  @Test
  public void testEncodeDecodeBuffer() {
    var text = getRandomText();
    ByteBuffer jsonBuffer =
        TabletMergeabilityUtil.encodeAsBuffer(text, TabletMergeability.after(Duration.ofDays(1)));
    var decoded = TabletMergeabilityUtil.decode(jsonBuffer);
    assertEquals(text, decoded.getFirst());
    assertEquals(TabletMergeability.after(Duration.ofDays(1)), decoded.getSecond());
  }

  private Text getRandomText() {
    return new Text(String.valueOf(UUID.randomUUID()).replaceAll("-", ""));
  }
}
