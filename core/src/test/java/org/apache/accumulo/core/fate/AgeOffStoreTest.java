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

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.fate.AgeOffStore.TimeSource;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
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
    var txStore1 = aoStore.reserve(txid1);
    txStore1.setStatus(TStatus.IN_PROGRESS);
    txStore1.unreserve(0, TimeUnit.MILLISECONDS);

    aoStore.ageOff();

    long txid2 = aoStore.create();
    var txStore2 = aoStore.reserve(txid2);
    txStore2.setStatus(TStatus.IN_PROGRESS);
    txStore2.setStatus(TStatus.FAILED);
    txStore2.unreserve(0, TimeUnit.MILLISECONDS);

    tts.time = 6;

    long txid3 = aoStore.create();
    var txStore3 = aoStore.reserve(txid3);
    txStore3.setStatus(TStatus.IN_PROGRESS);
    txStore3.setStatus(TStatus.SUCCESSFUL);
    txStore3.unreserve(0, TimeUnit.MILLISECONDS);

    Long txid4 = aoStore.create();

    aoStore.ageOff();

    assertEquals(Set.of(txid1, txid2, txid3, txid4), aoStore.list().collect(toSet()));

    tts.time = 15;

    aoStore.ageOff();

    assertEquals(Set.of(txid1, txid3, txid4), aoStore.list().collect(toSet()));

    tts.time = 30;

    aoStore.ageOff();

    assertEquals(Set.of(txid1), aoStore.list().collect(toSet()));
  }

  @Test
  public void testNonEmpty() throws InterruptedException, KeeperException {
    // test age off when source store starts off non empty

    TestTimeSource tts = new TestTimeSource();
    TestStore testStore = new TestStore();
    long txid1 = testStore.create();
    var txStore1 = testStore.reserve(txid1);
    txStore1.setStatus(TStatus.IN_PROGRESS);
    txStore1.unreserve(0, TimeUnit.MILLISECONDS);

    long txid2 = testStore.create();
    var txStore2 = testStore.reserve(txid2);
    txStore2.setStatus(TStatus.IN_PROGRESS);
    txStore2.setStatus(TStatus.FAILED);
    txStore2.unreserve(0, TimeUnit.MILLISECONDS);

    long txid3 = testStore.create();
    var txStore3 = testStore.reserve(txid3);
    txStore3.setStatus(TStatus.IN_PROGRESS);
    txStore3.setStatus(TStatus.SUCCESSFUL);
    txStore3.unreserve(0, TimeUnit.MILLISECONDS);

    Long txid4 = testStore.create();

    AgeOffStore<String> aoStore = new AgeOffStore<>(testStore, 10, tts);

    assertEquals(Set.of(txid1, txid2, txid3, txid4), aoStore.list().collect(toSet()));

    aoStore.ageOff();

    assertEquals(Set.of(txid1, txid2, txid3, txid4), aoStore.list().collect(toSet()));

    tts.time = 15;

    aoStore.ageOff();

    assertEquals(Set.of(txid1), aoStore.list().collect(toSet()));

    txStore1 = aoStore.reserve(txid1);
    txStore1.setStatus(TStatus.FAILED_IN_PROGRESS);
    txStore1.unreserve(0, TimeUnit.MILLISECONDS);

    tts.time = 30;

    aoStore.ageOff();

    assertEquals(Set.of(txid1), aoStore.list().collect(toSet()));

    txStore1 = aoStore.reserve(txid1);
    txStore1.setStatus(TStatus.FAILED);
    txStore1.unreserve(0, TimeUnit.MILLISECONDS);

    aoStore.ageOff();

    assertEquals(Set.of(txid1), aoStore.list().collect(toSet()));

    tts.time = 42;

    aoStore.ageOff();

    assertEquals(0, aoStore.list().count());
  }
}
