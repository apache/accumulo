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
package org.apache.accumulo.server.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.base.Strings;

public class ServerIteratorOptionsTest {
  @Test
  public void testStringNone() {
    var aconf = SiteConfiguration.empty()
        .withOverrides(
            Map.of(Property.GENERAL_SERVER_ITERATOR_OPTIONS_COMPRESSION_ALGO.getKey(), "none"))
        .build();

    String v1 = Strings.repeat("a", 100_000);
    String v2 = Strings.repeat("b", 110_000);

    IteratorSetting iterSetting = new IteratorSetting(100, "ti", "TestIter.class");

    ServerIteratorOptions.compressOption(aconf, iterSetting, "k1", v1);
    ServerIteratorOptions.compressOption(aconf, iterSetting, "k2", v2);

    assertEquals(3, iterSetting.getOptions().size());

    // should not copy string when compression type is none
    assertSame(v1, iterSetting.getOptions().get("k1"));
    assertSame(v2, iterSetting.getOptions().get("k2"));
    assertEquals("none", iterSetting.getOptions().get(ServerIteratorOptions.COMPRESSION_ALGO));

    // should not copy string when compression type is none
    assertSame(v1, ServerIteratorOptions.decompressOption(iterSetting.getOptions(), "k1"));
    assertSame(v2, ServerIteratorOptions.decompressOption(iterSetting.getOptions(), "k2"));
  }

  @Test
  public void testStringCompress() {
    var aconf = SiteConfiguration.empty()
        .withOverrides(
            Map.of(Property.GENERAL_SERVER_ITERATOR_OPTIONS_COMPRESSION_ALGO.getKey(), "gz"))
        .build();

    String v1 = Strings.repeat("a", 100_000);
    String v2 = Strings.repeat("b", 110_000);

    assertEquals(100_000, v1.length());
    assertEquals(110_000, v2.length());

    IteratorSetting iterSetting = new IteratorSetting(100, "ti", "TestIter.class");

    ServerIteratorOptions.compressOption(aconf, iterSetting, "k1", v1);
    ServerIteratorOptions.compressOption(aconf, iterSetting, "k2", v2);

    assertEquals(3, iterSetting.getOptions().size());

    // the stored value should be much smaller
    assertTrue(iterSetting.getOptions().get("k1").length() < 1000);
    assertTrue(iterSetting.getOptions().get("k2").length() < 1000);
    assertEquals("gz", iterSetting.getOptions().get(ServerIteratorOptions.COMPRESSION_ALGO));

    // should not copy string when compression type is none
    assertEquals(v1, ServerIteratorOptions.decompressOption(iterSetting.getOptions(), "k1"));
    assertEquals(v2, ServerIteratorOptions.decompressOption(iterSetting.getOptions(), "k2"));
  }

  @Test
  public void testSerializeNone() {
    var aconf = SiteConfiguration.empty()
        .withOverrides(
            Map.of(Property.GENERAL_SERVER_ITERATOR_OPTIONS_COMPRESSION_ALGO.getKey(), "none"))
        .build();

    IteratorSetting iterSetting = new IteratorSetting(100, "ti", "TestIter.class");

    Set<KeyExtent> extents = new HashSet<>();
    TableId tableId = TableId.of("1234");

    for (int i = 1; i < 100_000; i++) {
      var extent = new KeyExtent(tableId, new Text(String.format("%10d", i)),
          new Text(String.format("%10d", i - 1)));
      extents.add(extent);
    }

    ServerIteratorOptions.compressOption(aconf, iterSetting, "k1", dataOutput -> {
      dataOutput.writeInt(extents.size());
      for (var extent : extents) {
        extent.writeTo(dataOutput);
      }
    });

    // expected minimum size of data, will be larger
    int expMinSize = 100_000 * (4 + 10 + 10);
    System.out.println(iterSetting.getOptions().get("k1").length());
    assertTrue(iterSetting.getOptions().get("k1").length() > expMinSize);

    Set<KeyExtent> extents2 =
        ServerIteratorOptions.decompressOption(iterSetting.getOptions(), "k1", dataInput -> {
          int num = dataInput.readInt();
          HashSet<KeyExtent> es = new HashSet<>(num);
          for (int i = 0; i < num; i++) {
            es.add(KeyExtent.readFrom(dataInput));
          }
          return es;
        });

    assertEquals(extents, extents2);
    assertNotSame(extents, extents2);
  }

  @Test
  public void testSerializeGz() {
    var aconf = SiteConfiguration.empty()
        .withOverrides(
            Map.of(Property.GENERAL_SERVER_ITERATOR_OPTIONS_COMPRESSION_ALGO.getKey(), "gz"))
        .build();

    IteratorSetting iterSetting = new IteratorSetting(100, "ti", "TestIter.class");

    Set<KeyExtent> extents = new HashSet<>();
    TableId tableId = TableId.of("1234");

    for (int i = 1; i < 100_000; i++) {
      var extent = new KeyExtent(tableId, new Text(String.format("%10d", i)),
          new Text(String.format("%10d", i - 1)));
      extents.add(extent);
    }

    ServerIteratorOptions.compressOption(aconf, iterSetting, "k1", dataOutput -> {
      dataOutput.writeInt(extents.size());
      for (var extent : extents) {
        extent.writeTo(dataOutput);
      }
    });

    // expected minimum size of data
    int expMinSize = 100_000 * (4 + 10 + 10);
    System.out.println(iterSetting.getOptions().get("k1").length());
    // should be smaller than the expected min size because of compression
    assertTrue(iterSetting.getOptions().get("k1").length() < expMinSize);

    Set<KeyExtent> extents2 =
        ServerIteratorOptions.decompressOption(iterSetting.getOptions(), "k1", dataInput -> {
          int num = dataInput.readInt();
          HashSet<KeyExtent> es = new HashSet<>(num);
          for (int i = 0; i < num; i++) {
            es.add(KeyExtent.readFrom(dataInput));
          }
          return es;
        });

    assertEquals(extents, extents2);
  }
}
