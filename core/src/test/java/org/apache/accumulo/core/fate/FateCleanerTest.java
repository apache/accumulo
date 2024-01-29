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

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.fate.FateCleaner.TimeSource;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.FateIdStatus;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

public class FateCleanerTest {

  private static class TestTimeSource implements TimeSource {
    long time = 0;

    @Override
    public long currentTimeNanos() {
      return time;
    }

  }

  @Test
  public void testBasic() throws InterruptedException, KeeperException {

    TestTimeSource tts = new TestTimeSource();
    TestStore testStore = new TestStore();
    FateCleaner<String> cleaner = new FateCleaner<>(testStore, Duration.ofNanos(10), tts);

    cleaner.ageOff();

    long txid1 = testStore.create();
    var txStore1 = testStore.reserve(txid1);
    txStore1.setStatus(TStatus.IN_PROGRESS);
    txStore1.unreserve(0, TimeUnit.MILLISECONDS);

    cleaner.ageOff();

    long txid2 = testStore.create();
    var txStore2 = testStore.reserve(txid2);
    txStore2.setStatus(TStatus.IN_PROGRESS);
    txStore2.setStatus(TStatus.FAILED);
    txStore2.unreserve(0, TimeUnit.MILLISECONDS);

    cleaner.ageOff();

    tts.time = 6;

    long txid3 = testStore.create();
    var txStore3 = testStore.reserve(txid3);
    txStore3.setStatus(TStatus.IN_PROGRESS);
    txStore3.setStatus(TStatus.SUCCESSFUL);
    txStore3.unreserve(0, TimeUnit.MILLISECONDS);

    cleaner.ageOff();

    Long txid4 = testStore.create();

    cleaner.ageOff();

    assertEquals(Set.of(txid1, txid2, txid3, txid4),
        testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    tts.time = 15;

    cleaner.ageOff();

    assertEquals(Set.of(txid1, txid3, txid4),
        testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    tts.time = 30;

    cleaner.ageOff();

    assertEquals(Set.of(txid1), testStore.list().map(FateIdStatus::getTxid).collect(toSet()));
  }

  @Test
  public void testNonEmpty() {
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

    FateCleaner<String> cleaner = new FateCleaner<>(testStore, Duration.ofNanos(10), tts);
    cleaner.ageOff();

    assertEquals(Set.of(txid1, txid2, txid3, txid4),
        testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    cleaner.ageOff();

    assertEquals(Set.of(txid1, txid2, txid3, txid4),
        testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    tts.time = 15;

    cleaner.ageOff();

    assertEquals(Set.of(txid1), testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    txStore1 = testStore.reserve(txid1);
    txStore1.setStatus(TStatus.FAILED_IN_PROGRESS);
    txStore1.unreserve(0, TimeUnit.MILLISECONDS);

    tts.time = 30;

    cleaner.ageOff();

    assertEquals(Set.of(txid1), testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    txStore1 = testStore.reserve(txid1);
    txStore1.setStatus(TStatus.FAILED);
    txStore1.unreserve(0, TimeUnit.MILLISECONDS);

    cleaner.ageOff();

    assertEquals(Set.of(txid1), testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    tts.time = 42;

    cleaner.ageOff();

    assertEquals(0, testStore.list().count());
  }

  @Test
  public void testStatusChange() {
    // test ensure that if something is eligible for ageoff and its status changes it will no longer
    // be eligible

    TestTimeSource tts = new TestTimeSource();
    TestStore testStore = new TestStore();
    FateCleaner<String> cleaner = new FateCleaner<>(testStore, Duration.ofHours(10), tts);

    cleaner.ageOff();

    // create a something in the NEW state
    long txid1 = testStore.create();

    // create another that is complete
    long txid2 = testStore.create();
    var txStore2 = testStore.reserve(txid2);
    txStore2.setStatus(TStatus.IN_PROGRESS);
    txStore2.setStatus(TStatus.FAILED);
    txStore2.unreserve(0, TimeUnit.MILLISECONDS);

    // create another in the NEW state
    long txid3 = testStore.create();

    // start tracking what can age off, both should be candidates
    cleaner.ageOff();
    assertEquals(Set.of(txid1, txid2, txid3),
        testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    // advance time by 9 hours, nothing should age off
    tts.time += Duration.ofHours(9).toNanos();
    cleaner.ageOff();

    assertEquals(Set.of(txid1, txid2, txid3),
        testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    var txStore1 = testStore.reserve(txid1);
    txStore1.setStatus(TStatus.IN_PROGRESS);
    txStore1.setStatus(TStatus.FAILED);
    txStore1.unreserve(0, TimeUnit.MILLISECONDS);

    // advance time by 2 hours, both should be able to age off.. however the status changed on txid1
    // so it should not age off
    tts.time += Duration.ofHours(2).toNanos();
    cleaner.ageOff();

    assertEquals(Set.of(txid1), testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    // advance time by 9 hours, nothing should age off
    tts.time += Duration.ofHours(9).toNanos();
    cleaner.ageOff();
    assertEquals(Set.of(txid1), testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    // advance time by 2 hours, should age off everything
    tts.time += Duration.ofHours(2).toNanos();
    cleaner.ageOff();
    assertEquals(Set.of(), testStore.list().map(FateIdStatus::getTxid).collect(toSet()));
  }

  @Test
  public void testNewCleaner() {
    // this test ensures that a new cleaner instance ignores data from another cleaner instance

    TestTimeSource tts = new TestTimeSource();
    TestStore testStore = new TestStore();
    FateCleaner<String> cleaner1 = new FateCleaner<>(testStore, Duration.ofHours(10), tts);

    long txid1 = testStore.create();

    cleaner1.ageOff();
    assertEquals(Set.of(txid1), testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    tts.time += Duration.ofHours(5).toNanos();
    long txid2 = testStore.create();

    cleaner1.ageOff();
    assertEquals(Set.of(txid1, txid2),
        testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    tts.time += Duration.ofHours(6).toNanos();
    long txid3 = testStore.create();

    cleaner1.ageOff();
    assertEquals(Set.of(txid2, txid3),
        testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    // create a new cleaner, it should ignore any data stored by previous cleaner
    FateCleaner<String> cleaner2 = new FateCleaner<>(testStore, Duration.ofHours(10), tts);

    tts.time += Duration.ofHours(5).toNanos();
    // since this is a new cleaner instance, it should reset the clock
    cleaner2.ageOff();
    assertEquals(Set.of(txid2, txid3),
        testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    // since the clock was reset, advancing time should not age anything off
    tts.time += Duration.ofHours(9).toNanos();
    cleaner2.ageOff();
    assertEquals(Set.of(txid2, txid3),
        testStore.list().map(FateIdStatus::getTxid).collect(toSet()));

    // this should advance time enough to age everything off
    tts.time += Duration.ofHours(2).toNanos();
    cleaner2.ageOff();
    assertEquals(Set.of(), testStore.list().map(FateIdStatus::getTxid).collect(toSet()));
  }
}
