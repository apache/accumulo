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
package org.apache.accumulo.server.manager.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TabletStateChangeIteratorTest {

  // This is the algorithm used for encoding prior to 2.1.4. Duplicated here to test compatibility.
  private Map<String,String> oldEncode(Collection<KeyExtent> migrations) {
    DataOutputBuffer buffer = new DataOutputBuffer();
    try {
      for (KeyExtent extent : migrations) {
        extent.writeTo(buffer);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return Map.of(TabletStateChangeIterator.MIGRATIONS_OPTION,
        Base64.getEncoder().encodeToString(Arrays.copyOf(buffer.getData(), buffer.getLength())));
  }

  @Test
  public void testMigrations() {
    KeyExtent ke1 = new KeyExtent(TableId.of("1234"), null, null);
    KeyExtent ke2 = new KeyExtent(TableId.of("7890"), new Text("abc"), null);

    testMigrations(Set.of(), "none");
    testMigrations(Set.of(), "gz");
    testMigrations(Set.of(ke1, ke2), "none");
    testMigrations(Set.of(ke1, ke2), "gz");

    // when nothing is set for migrations in options map should return empty set
    assertEquals(Set.of(), TabletStateChangeIterator.parseMigrations(Map.of()));
  }

  private void testMigrations(Set<KeyExtent> migrations, String compAlgo) {
    var aconf = SiteConfiguration.empty()
        .withOverrides(
            Map.of(Property.GENERAL_SERVER_ITERATOR_OPTIONS_COMPRESSION_ALGO.getKey(), compAlgo))
        .build();
    IteratorSetting iteratorSetting = new IteratorSetting(100, "myiter", "MyIter.class");
    TabletStateChangeIterator.setMigrations(aconf, iteratorSetting, migrations);
    var migrations2 = TabletStateChangeIterator.parseMigrations(iteratorSetting.getOptions());
    assertEquals(migrations, migrations2);
    assertNotSame(migrations, migrations2);

    if (compAlgo.equals("none")) {
      // simulate 2.1.3 sending data
      var options = oldEncode(migrations);
      var migrations3 = TabletStateChangeIterator.parseMigrations(options);
      assertEquals(migrations, migrations3);
      assertNotSame(migrations, migrations3);
    }
  }
}
