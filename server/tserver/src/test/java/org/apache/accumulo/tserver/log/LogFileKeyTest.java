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
package org.apache.accumulo.tserver.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class LogFileKeyTest {

  private static LogFileKey nk(LogEvents e, int tabletId, long seq) {
    LogFileKey k = new LogFileKey();
    k.event = e;
    k.tabletId = tabletId;
    k.seq = seq;
    return k;
  }

  @Test
  public void testEquivalence() {

    LogFileKey start = nk(LogEvents.COMPACTION_START, 1, 3);
    LogFileKey finish = nk(LogEvents.COMPACTION_FINISH, 1, 3);
    LogFileKey mut = nk(LogEvents.MUTATION, 1, 3);
    LogFileKey mmut = nk(LogEvents.MANY_MUTATIONS, 1, 3);

    assertEquals(0, start.compareTo(finish));
    assertEquals(0, finish.compareTo(start));

    assertEquals(0, mut.compareTo(mmut));
    assertEquals(0, mmut.compareTo(mut));
  }

  @Test
  public void testSortOrder() {
    List<LogFileKey> keys = new ArrayList<>();

    // add keys in expected sort order
    keys.add(nk(LogEvents.OPEN, 0, 0));

    // there was a bug that was putting -1 in WAL for tabletId, ensure this data sorts as expected
    keys.add(nk(LogEvents.DEFINE_TABLET, -1, 0));
    keys.add(nk(LogEvents.DEFINE_TABLET, 3, 6));
    keys.add(nk(LogEvents.DEFINE_TABLET, 3, 7));
    keys.add(nk(LogEvents.DEFINE_TABLET, 4, 2));
    keys.add(nk(LogEvents.DEFINE_TABLET, 4, 9));

    keys.add(nk(LogEvents.COMPACTION_START, 3, 3));
    keys.add(nk(LogEvents.COMPACTION_FINISH, 3, 5));
    keys.add(nk(LogEvents.COMPACTION_START, 3, 7));
    keys.add(nk(LogEvents.COMPACTION_FINISH, 3, 9));

    keys.add(nk(LogEvents.COMPACTION_START, 4, 1));
    keys.add(nk(LogEvents.COMPACTION_FINISH, 4, 3));
    keys.add(nk(LogEvents.COMPACTION_START, 4, 11));
    keys.add(nk(LogEvents.COMPACTION_FINISH, 4, 13));

    keys.add(nk(LogEvents.MUTATION, 3, 1));
    keys.add(nk(LogEvents.MUTATION, 3, 2));
    keys.add(nk(LogEvents.MUTATION, 3, 3));
    keys.add(nk(LogEvents.MUTATION, 3, 3));
    keys.add(nk(LogEvents.MANY_MUTATIONS, 3, 11));

    keys.add(nk(LogEvents.MANY_MUTATIONS, 4, 2));
    keys.add(nk(LogEvents.MUTATION, 4, 3));
    keys.add(nk(LogEvents.MUTATION, 4, 5));
    keys.add(nk(LogEvents.MUTATION, 4, 7));
    keys.add(nk(LogEvents.MANY_MUTATIONS, 4, 15));

    for (int i = 0; i < 10; i++) {
      List<LogFileKey> testList = new ArrayList<>(keys);
      Collections.shuffle(testList);
      Collections.sort(testList);

      assertEquals(keys, testList);
    }

  }

  private void testToFromKey(int tabletId, long seq) throws IOException {
    var logFileKey = nk(LogEvents.DEFINE_TABLET, tabletId, seq);
    logFileKey.tablet = new KeyExtent(TableId.of("5"), new Text("b"), null);
    Key k = logFileKey.toKey();

    var converted = LogFileKey.fromKey(k);
    assertEquals(logFileKey, converted,
        "Failed to convert tabletId = " + tabletId + " seq = " + seq);
  }

  @Test
  public void convertToKeyLargeTabletId() throws IOException {
    // test continuous range of numbers to make sure all cases of signed bytes are covered
    for (int i = -1_000; i < 1_000_000; i++) {
      testToFromKey(i, 1);
    }
  }

  @Test
  public void convertToKeyLargeSeq() throws IOException {
    // use numbers large enough to cover all bytes used
    testToFromKey(1, 8); // 1 byte
    testToFromKey(1, 1_000L); // 2 bytes
    testToFromKey(1, 1_222_333L); // 3 bytes
    testToFromKey(1, 100_000_000L); // 4 bytes
    testToFromKey(1, 111_222_333_444L); // 5 bytes
    testToFromKey(1, 1_000_222_333_777L); // 6 bytes
    testToFromKey(1, 5_222_000_222_333_777L); // 7 bytes
    testToFromKey(1, 500_222_333_444_555_666L); // 8 bytes

    // test with more tabletId bytes
    testToFromKey(-1000, 500_222_333_444_555_666L);
    testToFromKey(10_000_000, 500_222_333_444_555_666L);
    testToFromKey(Integer.MAX_VALUE, Long.MAX_VALUE);
    testToFromKey(Integer.MIN_VALUE, Long.MAX_VALUE);
    testToFromKey(Integer.MIN_VALUE, 0);
    testToFromKey(Integer.MAX_VALUE, 0);
  }

  @Test
  public void testNegativeSeq() {
    assertThrows(IllegalArgumentException.class, () -> testToFromKey(1, -1));
  }
}
