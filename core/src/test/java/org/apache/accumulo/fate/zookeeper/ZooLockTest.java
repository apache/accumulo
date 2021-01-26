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
package org.apache.accumulo.fate.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class ZooLockTest {

  @Test
  public void testSortAndFindLowestPrevPrefix() throws Exception {
    List<String> children = new ArrayList<>();
    children.add("zlock#00000000-0000-0000-0000-FFFFFFFFFFFF#0000000007");
    children.add("zlock#00000000-0000-0000-0000-EEEEEEEEEEEE#00000000010");
    children.add("zlock#00000000-0000-0000-0000-BBBBBBBBBBBB#0000000006");
    children.add("zlock#00000000-0000-0000-0000-GGGGGGGGGGGG#0000000008");
    children.add("zlock#00000000-0000-0000-0000-BBBBBBBBBBBB#0000000004");
    children.add("zlock-123456789");
    children.add("zlock#00000000-0000-0000-0000-CCCCCCCCCCCC#0000000003");
    children.add("zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000002");
    children.add("zlock-987654321");
    children.add("zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000001");

    ZooLock.sortChildrenByLockPrefix(children);

    assertEquals(10, children.size());
    assertEquals("zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000001", children.get(0));
    assertEquals("zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000002", children.get(1));
    assertEquals("zlock#00000000-0000-0000-0000-CCCCCCCCCCCC#0000000003", children.get(2));
    assertEquals("zlock#00000000-0000-0000-0000-BBBBBBBBBBBB#0000000004", children.get(3));
    assertEquals("zlock#00000000-0000-0000-0000-BBBBBBBBBBBB#0000000006", children.get(4));
    assertEquals("zlock#00000000-0000-0000-0000-FFFFFFFFFFFF#0000000007", children.get(5));
    assertEquals("zlock#00000000-0000-0000-0000-GGGGGGGGGGGG#0000000008", children.get(6));
    assertEquals("zlock#00000000-0000-0000-0000-EEEEEEEEEEEE#00000000010", children.get(7));
    assertEquals("zlock-123456789", children.get(8));
    assertEquals("zlock-987654321", children.get(9));

    assertEquals("zlock#00000000-0000-0000-0000-BBBBBBBBBBBB#0000000004", ZooLock
        .findLowestPrevPrefix(children, "zlock#00000000-0000-0000-0000-FFFFFFFFFFFF#0000000007"));

    assertEquals("zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000001", ZooLock
        .findLowestPrevPrefix(children, "zlock#00000000-0000-0000-0000-CCCCCCCCCCCC#0000000003"));

    assertEquals("zlock#00000000-0000-0000-0000-GGGGGGGGGGGG#0000000008", ZooLock
        .findLowestPrevPrefix(children, "zlock#00000000-0000-0000-0000-EEEEEEEEEEEE#00000000010"));

    assertThrows(IndexOutOfBoundsException.class, () -> {
      ZooLock.findLowestPrevPrefix(children,
          "zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000001");
    });

    assertThrows(IndexOutOfBoundsException.class, () -> {
      ZooLock.findLowestPrevPrefix(children,
          "zlock#00000000-0000-0000-0000-XXXXXXXXXXXX#0000000099");
    });
  }

}
