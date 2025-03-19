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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.Test;

public class TabletManagementIteratorTest {

  @Test
  public void testRanges() throws IOException {
    TabletManagementIterator iter = new TabletManagementIterator();

    // We don't call init, so expect a IllegalStateException on success and
    // and IllegalArgumentException on failure
    Key goodStartKey = new Key("row");
    Key goodEndKey = new Key("rowEnd");

    Key badStartKey = new Key("row", "colf", "colq", 1234L);
    Key badEndKey = new Key("rowEnd", "colf", "colq", 1234L);

    assertThrows(NullPointerException.class, () -> iter.seek(null, Set.of(), false));
    assertThrows(NullPointerException.class,
        () -> iter.seek(new Range((Key) null, (Key) null), Set.of(), false));
    assertThrows(NullPointerException.class,
        () -> iter.seek(new Range(goodStartKey, goodEndKey), Set.of(), false));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> iter.seek(new Range(goodStartKey, badEndKey), Set.of(), false)).getMessage()
        .startsWith("TabletManagementIterator must be seeked"));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> iter.seek(new Range(badStartKey, goodEndKey), Set.of(), false)).getMessage()
        .startsWith("TabletManagementIterator must be seeked"));
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> iter.seek(new Range(badStartKey, badEndKey), Set.of(), false)).getMessage()
        .startsWith("TabletManagementIterator must be seeked"));
  }
}
