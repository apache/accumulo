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
package org.apache.accumulo.core.fate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.fate.AgeOffStore.TimeSource;
import org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

public class AgeOffStoreTest {

  private static class TestTimeSource implements TimeSource {
    long time = 0;

    @Override
    public long currentTimeMillis() {
      return time;
    }

  }

  @Test
  public void testBasic() throws InterruptedException, KeeperException {

    TestTimeSource tts = new TestTimeSource();
    TestStore testStore = new TestStore();
    AgeOffStore<String> aoStore = new AgeOffStore<>(testStore, 10, tts);

    aoStore.ageOff();

    long txid1 = aoStore.create();
    aoStore.reserve(txid1);
    aoStore.setStatus(txid1, TStatus.IN_PROGRESS);
    aoStore.unreserve(txid1, 0);

    aoStore.ageOff();

    long txid2 = aoStore.create();
    aoStore.reserve(txid2);
    aoStore.setStatus(txid2, TStatus.IN_PROGRESS);
    aoStore.setStatus(txid2, TStatus.FAILED);
    aoStore.unreserve(txid2, 0);

    tts.time = 6;

    long txid3 = aoStore.create();
    aoStore.reserve(txid3);
    aoStore.setStatus(txid3, TStatus.IN_PROGRESS);
    aoStore.setStatus(txid3, TStatus.SUCCESSFUL);
    aoStore.unreserve(txid3, 0);

    Long txid4 = aoStore.create();

    aoStore.ageOff();

    assertEquals(Set.of(txid1, txid2, txid3, txid4), new HashSet<>(aoStore.list()));
    assertEquals(4, new HashSet<>(aoStore.list()).size());

    tts.time = 15;

    aoStore.ageOff();

    assertEquals(Set.of(txid1, txid3, txid4), new HashSet<>(aoStore.list()));
    assertEquals(3, new HashSet<>(aoStore.list()).size());

    tts.time = 30;

    aoStore.ageOff();

    assertEquals(Set.of(txid1), new HashSet<>(aoStore.list()));
    assertEquals(1, Set.of(aoStore.list()).size());
  }

  @Test
  public void testNonEmpty() throws InterruptedException, KeeperException {
    // test age off when source store starts off non empty

    TestTimeSource tts = new TestTimeSource();
    TestStore testStore = new TestStore();
    long txid1 = testStore.create();
    testStore.reserve(txid1);
    testStore.setStatus(txid1, TStatus.IN_PROGRESS);
    testStore.unreserve(txid1, 0);

    long txid2 = testStore.create();
    testStore.reserve(txid2);
    testStore.setStatus(txid2, TStatus.IN_PROGRESS);
    testStore.setStatus(txid2, TStatus.FAILED);
    testStore.unreserve(txid2, 0);

    long txid3 = testStore.create();
    testStore.reserve(txid3);
    testStore.setStatus(txid3, TStatus.IN_PROGRESS);
    testStore.setStatus(txid3, TStatus.SUCCESSFUL);
    testStore.unreserve(txid3, 0);

    Long txid4 = testStore.create();

    AgeOffStore<String> aoStore = new AgeOffStore<>(testStore, 10, tts);

    assertEquals(Set.of(txid1, txid2, txid3, txid4), new HashSet<>(aoStore.list()));
    assertEquals(4, new HashSet<>(aoStore.list()).size());

    aoStore.ageOff();

    assertEquals(Set.of(txid1, txid2, txid3, txid4), new HashSet<>(aoStore.list()));
    assertEquals(4, new HashSet<>(aoStore.list()).size());

    tts.time = 15;

    aoStore.ageOff();

    assertEquals(Set.of(txid1), new HashSet<>(aoStore.list()));
    assertEquals(1, new HashSet<>(aoStore.list()).size());

    aoStore.reserve(txid1);
    aoStore.setStatus(txid1, TStatus.FAILED_IN_PROGRESS);
    aoStore.unreserve(txid1, 0);

    tts.time = 30;

    aoStore.ageOff();

    assertEquals(Set.of(txid1), new HashSet<>(aoStore.list()));
    assertEquals(1, new HashSet<>(aoStore.list()).size());

    aoStore.reserve(txid1);
    aoStore.setStatus(txid1, TStatus.FAILED);
    aoStore.unreserve(txid1, 0);

    aoStore.ageOff();

    assertEquals(Set.of(txid1), new HashSet<>(aoStore.list()));
    assertEquals(1, new HashSet<>(aoStore.list()).size());

    tts.time = 42;

    aoStore.ageOff();

    assertEquals(0, new HashSet<>(aoStore.list()).size());
  }
}
