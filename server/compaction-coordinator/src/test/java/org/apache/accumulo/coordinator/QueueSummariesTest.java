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
package org.apache.accumulo.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.coordinator.QueueSummaries.PrioTserver;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.junit.jupiter.api.Test;

public class QueueSummariesTest {

  private TServerInstance ntsi(String tserver) {
    return new TServerInstance(tserver + ":9997", 0);
  }

  private PrioTserver npt(String tserver, short prio) {
    return new PrioTserver(ntsi(tserver), prio);
  }

  private void update(QueueSummaries queueSum, String tserver, String... data) {

    TServerInstance tsi = ntsi(tserver);

    List<TCompactionQueueSummary> summaries = new ArrayList<>();

    for (int i = 0; i < data.length; i += 2) {
      summaries.add(new TCompactionQueueSummary(data[i], Short.parseShort(data[i + 1])));
    }

    queueSum.update(tsi, summaries);
  }

  @Test
  public void testBasic() {
    QueueSummaries queueSum = new QueueSummaries();

    update(queueSum, "ts1", "q1", "5", "q1", "4", "q2", "5", "q3", "4");
    update(queueSum, "ts2", "q1", "5", "q3", "5", "q3", "4");
    update(queueSum, "ts3", "q1", "5", "q2", "5");

    for (int i = 0; i < 3; i++) {
      assertEquals(npt("ts1", (short) 5), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts1", (short) 5), queueSum.getNextTserver("q2"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts3", (short) 5), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts3", (short) 5), queueSum.getNextTserver("q2"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q3"));
    }

    queueSum.removeSummary(ntsi("ts2"), "q1", (short) 5);

    for (int i = 0; i < 3; i++) {
      assertEquals(npt("ts1", (short) 5), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts1", (short) 5), queueSum.getNextTserver("q2"));
      assertEquals(npt("ts3", (short) 5), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts3", (short) 5), queueSum.getNextTserver("q2"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q3"));
    }

    queueSum.removeSummary(ntsi("ts3"), "q2", (short) 5);
    queueSum.removeSummary(ntsi("ts2"), "q3", (short) 5);

    for (int i = 0; i < 3; i++) {
      assertEquals(npt("ts1", (short) 5), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts1", (short) 5), queueSum.getNextTserver("q2"));
      assertEquals(npt("ts3", (short) 5), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts1", (short) 4), queueSum.getNextTserver("q3"));
      assertEquals(npt("ts2", (short) 4), queueSum.getNextTserver("q3"));
    }

  }

  @Test
  public void testUpdate() {
    QueueSummaries queueSum = new QueueSummaries();

    update(queueSum, "ts1", "q1", "5", "q2", "5", "q3", "4");
    update(queueSum, "ts2", "q1", "5", "q2", "4", "q3", "5");

    for (int i = 0; i < 3; i++) {
      assertEquals(npt("ts1", (short) 5), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts1", (short) 5), queueSum.getNextTserver("q2"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q3"));
    }

    // an update from the tserver should remove some existing entries
    update(queueSum, "ts1", "q1", "4", "q2", "6");
    update(queueSum, "ts2", "q1", "7", "q3", "3", "q4", "5");

    for (int i = 0; i < 3; i++) {
      assertEquals(npt("ts2", (short) 7), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts1", (short) 6), queueSum.getNextTserver("q2"));
      assertEquals(npt("ts2", (short) 3), queueSum.getNextTserver("q3"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q4"));
    }

    queueSum.removeSummary(ntsi("ts2"), "q1", (short) 7);
    queueSum.removeSummary(ntsi("ts1"), "q2", (short) 6);

    for (int i = 0; i < 3; i++) {
      assertEquals(npt("ts1", (short) 4), queueSum.getNextTserver("q1"));
      assertNull(queueSum.getNextTserver("q2"));
      assertEquals(npt("ts2", (short) 3), queueSum.getNextTserver("q3"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q4"));
    }
  }

  @Test
  public void testRemoveTserver() {
    QueueSummaries queueSum = new QueueSummaries();

    update(queueSum, "ts1", "q1", "5", "q2", "5", "q3", "4");
    update(queueSum, "ts2", "q1", "5", "q2", "4", "q3", "5");

    for (int i = 0; i < 3; i++) {
      assertEquals(npt("ts1", (short) 5), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts1", (short) 5), queueSum.getNextTserver("q2"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q3"));
    }

    queueSum.remove(Set.of(ntsi("ts1")));

    for (int i = 0; i < 3; i++) {
      assertEquals(npt("ts2", (short) 4), queueSum.getNextTserver("q2"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q1"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q3"));
    }

    queueSum.removeSummary(ntsi("ts2"), "q3", (short) 5);

    for (int i = 0; i < 3; i++) {
      assertEquals(npt("ts2", (short) 4), queueSum.getNextTserver("q2"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q1"));
      assertNull(queueSum.getNextTserver("q3"));
    }

    update(queueSum, "ts1", "q2", "6", "q1", "3");

    for (int i = 0; i < 3; i++) {
      assertEquals(npt("ts1", (short) 6), queueSum.getNextTserver("q2"));
      assertEquals(npt("ts2", (short) 5), queueSum.getNextTserver("q1"));
      assertNull(queueSum.getNextTserver("q3"));
    }

    queueSum.remove(Set.of(ntsi("ts2")));

    for (int i = 0; i < 3; i++) {
      assertEquals(npt("ts1", (short) 6), queueSum.getNextTserver("q2"));
      assertEquals(npt("ts1", (short) 3), queueSum.getNextTserver("q1"));
      assertNull(queueSum.getNextTserver("q3"));
    }

    queueSum.removeSummary(ntsi("ts1"), "q2", (short) 6);

    for (int i = 0; i < 3; i++) {
      assertEquals(npt("ts1", (short) 3), queueSum.getNextTserver("q1"));
      assertNull(queueSum.getNextTserver("q2"));
      assertNull(queueSum.getNextTserver("q3"));
    }

    queueSum.remove(Set.of(ntsi("ts1")));

    for (int i = 0; i < 3; i++) {
      assertNull(queueSum.getNextTserver("q1"));
      assertNull(queueSum.getNextTserver("q2"));
      assertNull(queueSum.getNextTserver("q3"));
    }
  }
}
