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
package org.apache.accumulo.fate;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.accumulo.fate.AgeOffStore.TimeSource;
import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class AgeOffStoreTest {

  private static class TestTimeSource implements TimeSource {
    long time = 0;

    @Override
    public long currentTimeMillis() {
      return time;
    }

  }

  @Test
  public void testBasic() {

    TestTimeSource tts = new TestTimeSource();
    SimpleStore<String> sstore = new SimpleStore<>();
    AgeOffStore<String> aoStore = new AgeOffStore<>(sstore, 10, tts);

    aoStore.ageOff();

    Long txid1 = aoStore.create();
    aoStore.reserve(txid1);
    aoStore.setStatus(txid1, TStatus.IN_PROGRESS);
    aoStore.unreserve(txid1, 0);

    aoStore.ageOff();

    Long txid2 = aoStore.create();
    aoStore.reserve(txid2);
    aoStore.setStatus(txid2, TStatus.IN_PROGRESS);
    aoStore.setStatus(txid2, TStatus.FAILED);
    aoStore.unreserve(txid2, 0);

    tts.time = 6;

    Long txid3 = aoStore.create();
    aoStore.reserve(txid3);
    aoStore.setStatus(txid3, TStatus.IN_PROGRESS);
    aoStore.setStatus(txid3, TStatus.SUCCESSFUL);
    aoStore.unreserve(txid3, 0);

    Long txid4 = aoStore.create();

    aoStore.ageOff();

    Assert.assertEquals(new HashSet<>(Arrays.asList(txid1, txid2, txid3, txid4)), new HashSet<>(aoStore.list()));
    Assert.assertEquals(4, new HashSet<>(aoStore.list()).size());

    tts.time = 15;

    aoStore.ageOff();

    Assert.assertEquals(new HashSet<>(Arrays.asList(txid1, txid3, txid4)), new HashSet<>(aoStore.list()));
    Assert.assertEquals(3, new HashSet<>(aoStore.list()).size());

    tts.time = 30;

    aoStore.ageOff();

    Assert.assertEquals(new HashSet<>(Arrays.asList(txid1)), new HashSet<>(aoStore.list()));
    Assert.assertEquals(1, new HashSet<>(aoStore.list()).size());
  }

  @Test
  public void testNonEmpty() {
    // test age off when source store starts off non empty

    TestTimeSource tts = new TestTimeSource();
    SimpleStore<String> sstore = new SimpleStore<>();
    Long txid1 = sstore.create();
    sstore.reserve(txid1);
    sstore.setStatus(txid1, TStatus.IN_PROGRESS);
    sstore.unreserve(txid1, 0);

    Long txid2 = sstore.create();
    sstore.reserve(txid2);
    sstore.setStatus(txid2, TStatus.IN_PROGRESS);
    sstore.setStatus(txid2, TStatus.FAILED);
    sstore.unreserve(txid2, 0);

    Long txid3 = sstore.create();
    sstore.reserve(txid3);
    sstore.setStatus(txid3, TStatus.IN_PROGRESS);
    sstore.setStatus(txid3, TStatus.SUCCESSFUL);
    sstore.unreserve(txid3, 0);

    Long txid4 = sstore.create();

    AgeOffStore<String> aoStore = new AgeOffStore<>(sstore, 10, tts);

    Assert.assertEquals(new HashSet<>(Arrays.asList(txid1, txid2, txid3, txid4)), new HashSet<>(aoStore.list()));
    Assert.assertEquals(4, new HashSet<>(aoStore.list()).size());

    aoStore.ageOff();

    Assert.assertEquals(new HashSet<>(Arrays.asList(txid1, txid2, txid3, txid4)), new HashSet<>(aoStore.list()));
    Assert.assertEquals(4, new HashSet<>(aoStore.list()).size());

    tts.time = 15;

    aoStore.ageOff();

    Assert.assertEquals(new HashSet<>(Arrays.asList(txid1)), new HashSet<>(aoStore.list()));
    Assert.assertEquals(1, new HashSet<>(aoStore.list()).size());

    aoStore.reserve(txid1);
    aoStore.setStatus(txid1, TStatus.FAILED_IN_PROGRESS);
    aoStore.unreserve(txid1, 0);

    tts.time = 30;

    aoStore.ageOff();

    Assert.assertEquals(new HashSet<>(Arrays.asList(txid1)), new HashSet<>(aoStore.list()));
    Assert.assertEquals(1, new HashSet<>(aoStore.list()).size());

    aoStore.reserve(txid1);
    aoStore.setStatus(txid1, TStatus.FAILED);
    aoStore.unreserve(txid1, 0);

    aoStore.ageOff();

    Assert.assertEquals(new HashSet<>(Arrays.asList(txid1)), new HashSet<>(aoStore.list()));
    Assert.assertEquals(1, new HashSet<>(aoStore.list()).size());

    tts.time = 42;

    aoStore.ageOff();

    Assert.assertEquals(0, new HashSet<>(aoStore.list()).size());
  }
}
