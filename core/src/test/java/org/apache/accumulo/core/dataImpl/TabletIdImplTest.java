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
package org.apache.accumulo.core.dataImpl;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.TableId;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

class TabletIdImplTest {

  @Test
  void testHashCode() {
    // Testing consistency
    TabletIdImpl tabletId =
        new TabletIdImpl(new KeyExtent(TableId.of("r1"), new Text("b"), new Text("a")));
    int hashCode1 = tabletId.hashCode();
    int hashCode2 = tabletId.hashCode();
    assertEquals(hashCode1, hashCode2);

    // Testing equality
    TabletIdImpl tabletOne =
        new TabletIdImpl(new KeyExtent(TableId.of("r1"), new Text("b"), new Text("a")));
    TabletIdImpl tabletTwo =
        new TabletIdImpl(new KeyExtent(TableId.of("r1"), new Text("b"), new Text("a")));
    assertEquals(tabletOne.hashCode(), tabletTwo.hashCode());

    // Testing even distribution
    List<TabletIdImpl> tablets = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      tablets.add(new TabletIdImpl(
          new KeyExtent(TableId.of("r1" + i), new Text("b" + i), new Text("a" + i))));
    }
    Set<Integer> hashCodes = new HashSet<>();
    for (TabletIdImpl tabs : tablets) {
      hashCodes.add(tabs.hashCode());
    }
    assertEquals(tablets.size(), hashCodes.size(), 10);
  }
}
