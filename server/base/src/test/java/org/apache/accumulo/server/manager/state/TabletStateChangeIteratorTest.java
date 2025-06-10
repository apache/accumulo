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

import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TabletStateChangeIteratorTest {

  // This is the algorithm used for encoding prior to 2.1.4.  Duplicated here to test compatibility.
  private String oldEncode(Collection<KeyExtent> migrations){
    DataOutputBuffer buffer = new DataOutputBuffer();
    try {
      for (KeyExtent extent : migrations) {
        extent.writeTo(buffer);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
      return Base64.getEncoder().encodeToString(Arrays.copyOf(buffer.getData(), buffer.getLength()));
  }

  @Test
  public void testEncodeMigrations() {
    Text prev = new Text(String.format("%09d", 0));
    TableId tableId = TableId.of("1234");
    HashSet<KeyExtent> migrations = new HashSet<>();
    for (int i = 1; i < 100_000; i++) {
      Text end = new Text(String.format("%09d", i));
      migrations.add(new KeyExtent(tableId, end, prev));
      prev = end;
    }

    for(var algo : List.of("none","gz")) {
      testEncodeDecode(migrations, algo);
      testEncodeDecode(Set.of(), algo);
    }
  }

  private void testEncodeDecode(Collection<KeyExtent> migrations, String algo) {
    String encodeMigrations =
            TabletStateChangeIterator.encodeMigrations(Collections.unmodifiableCollection(migrations), algo);
    assertEquals(migrations, TabletStateChangeIterator.decodeMigrations(encodeMigrations, algo));
    if(algo.equals("none")){
      assertEquals(encodeMigrations, oldEncode(migrations));
    }
  }
}
