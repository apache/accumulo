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
package org.apache.accumulo.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class InternerTest {

  private class TestObj {
    private final int id;

    TestObj(int id) {
      this.id = id;
    }

    @Override
    public int hashCode() {
      return id;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof TestObj) {
        return ((TestObj) obj).id == this.id;
      }
      return false;
    }
  }

  @Test
  public void testInternDedupes() {
    var interner = new Interner<TestObj>();

    var obj1 = new TestObj(1);
    var obj1_dupe = new TestObj(1);
    assertSame(obj1, obj1);
    assertNotSame(obj1, obj1_dupe);
    assertEquals(obj1, obj1_dupe);
    assertEquals(obj1.hashCode(), obj1_dupe.hashCode());

    // verify object gets added to the intern pool
    assertSame(obj1, interner.intern(obj1));
    assertEquals(1, interner.size());

    // verify equivalent, but not the same object, gets deduplicated
    assertSame(obj1, interner.intern(obj1_dupe));
    assertEquals(1, interner.size());

    // verify second object grows the intern pool size
    var obj2 = new TestObj(2);
    assertNotSame(obj1, obj2);
    assertNotEquals(obj1, obj2);
    var intern2 = interner.intern(obj2);
    assertEquals(2, interner.size());

    // sanity check to ensure we got the same object back for obj2, and it's not mangled with obj1
    assertSame(obj2, intern2);
    assertNotEquals(obj1, intern2);
  }

  @Test
  @Timeout(20)
  public void testInternsGetGarbageCollected() {
    var interner = new Interner<TestObj>();
    assertEquals(0, interner.size()); // ensure empty

    // add one and keep a strong reference
    var obj1 = interner.intern(new TestObj(1));
    assertEquals(1, interner.size());

    // try to add a second, weakly referenced object until it sticks (may be GC'd between checks)
    do {
      interner.intern(new TestObj(2));
    } while (interner.size() != 2);

    // best effort to GC until the weakly reachable object goes away or until test times out
    do {
      System.gc();
    } while (interner.size() != 1);

    // ensure obj1 is still interned (because we kept a strong reference)
    assertSame(obj1, interner.intern(new TestObj(1)));

    // ensure second test object is entirely new (previous ones should have been GC'd)
    var obj2 = new TestObj(2);
    assertSame(obj2, interner.intern(obj2));
    assertEquals(2, interner.size());
  }

}
