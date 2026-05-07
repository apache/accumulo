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
package org.apache.accumulo.core.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class AbstractIdTest {

  @Test
  void testHashCode() {
    // Testing consistency
    TestAbstractId abstractId = new TestAbstractId("value");
    int hashOne = abstractId.hashCode();
    int hashTwo = abstractId.hashCode();
    assertEquals(hashOne, hashTwo);

    // Testing equality
    TestAbstractId abstractOne = new TestAbstractId("value");
    TestAbstractId abstractTwo = new TestAbstractId("value");
    assertEquals(abstractOne.hashCode(), abstractTwo.hashCode());

    // Testing even distribution
    List<TestAbstractId> idList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      idList.add(new TestAbstractId("value" + i));
    }
    Set<Integer> hashCodes = new HashSet<>();
    for (TestAbstractId id : idList) {
      hashCodes.add(id.hashCode());
    }
    assertEquals(idList.size(), hashCodes.size(), 10);
  }

  private static class TestAbstractId extends AbstractId<TestAbstractId> {
    private static final long serialVersionUID = 1L;

    protected TestAbstractId(String canonical) {
      super(canonical);
    }
  }
}
