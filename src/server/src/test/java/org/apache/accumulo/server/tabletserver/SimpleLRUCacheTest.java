/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.tabletserver;

import static org.junit.Assert.*;

import org.junit.Test;

public class SimpleLRUCacheTest {

  @Test
  public void test() {
    SimpleLRUCache<Integer> test = new SimpleLRUCache<Integer>(4);
    test.add(0);
    assertTrue(test.contains(0));
    test.add(1);
    assertTrue(test.contains(1));
    assertFalse(test.contains(2));
    test.add(2);
    test.add(3);
    test.add(4);
    assertFalse(test.contains(0));
    test.add(2);
    test.add(2);
    test.add(2);
    test.add(2);
    assertTrue(test.contains(3));
    assertTrue(test.contains(4));
  }
}
