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
package org.apache.accumulo.server.util.fateCommand;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.ReadOnlyTStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TxnDetailsTest {

  private final static Logger log = LoggerFactory.getLogger(FateTxnDetails.class);

  @Test
  void orderingByDuration() {
    Map<String,String> idMap = Map.of("1", "ns1", "2", "tbl1");

    long now = System.currentTimeMillis();

    AdminUtil.TransactionStatus status1 = createMock(AdminUtil.TransactionStatus.class);
    expect(status1.getTimeCreated()).andReturn(now - TimeUnit.DAYS.toMillis(1)).anyTimes();
    expect(status1.getStatus()).andReturn(ReadOnlyTStore.TStatus.IN_PROGRESS).anyTimes();
    expect(status1.getTop()).andReturn("step1").anyTimes();
    expect(status1.getTxName()).andReturn("runningTx1").anyTimes();
    expect(status1.getTxid()).andReturn("abcdabcd").anyTimes();
    expect(status1.getHeldLocks()).andReturn(List.of()).anyTimes();
    expect(status1.getWaitingLocks()).andReturn(List.of()).anyTimes();

    AdminUtil.TransactionStatus status2 = createMock(AdminUtil.TransactionStatus.class);
    expect(status2.getTimeCreated()).andReturn(now - TimeUnit.DAYS.toMillis(7)).anyTimes();
    expect(status2.getStatus()).andReturn(ReadOnlyTStore.TStatus.IN_PROGRESS).anyTimes();
    expect(status2.getTop()).andReturn("step2").anyTimes();
    expect(status2.getTxName()).andReturn("runningTx2").anyTimes();
    expect(status2.getTxid()).andReturn("123456789").anyTimes();
    expect(status2.getHeldLocks()).andReturn(List.of()).anyTimes();
    expect(status2.getWaitingLocks()).andReturn(List.of()).anyTimes();

    replay(status1, status2);

    FateTxnDetails txn1 = new FateTxnDetails(System.currentTimeMillis(), status1, idMap);
    FateTxnDetails txn2 = new FateTxnDetails(System.currentTimeMillis(), status2, idMap);

    Set<FateTxnDetails> sorted = new TreeSet<>();
    sorted.add(txn1);
    sorted.add(txn2);

    log.trace("Sorted: {}", sorted);

    Iterator<FateTxnDetails> itor = sorted.iterator();

    assertTrue(itor.next().toString().contains("123456789"));
    assertTrue(itor.next().toString().contains("abcdabcd"));

    verify(status1, status2);
  }

  @Test
  public void lockTest() {
    Map<String,String> idMap = Map.of("1", "ns1", "2", "tbl1", "3", "", "4", "");

    long now = System.currentTimeMillis();

    AdminUtil.TransactionStatus status1 = createMock(AdminUtil.TransactionStatus.class);
    expect(status1.getTimeCreated()).andReturn(now - TimeUnit.DAYS.toMillis(1)).anyTimes();
    expect(status1.getStatus()).andReturn(ReadOnlyTStore.TStatus.IN_PROGRESS).anyTimes();
    expect(status1.getTop()).andReturn("step1").anyTimes();
    expect(status1.getTxName()).andReturn("runningTx").anyTimes();
    expect(status1.getTxid()).andReturn("abcdabcd").anyTimes();
    // incomplete lock info (W unknown ns id, no table))
    expect(status1.getHeldLocks()).andReturn(List.of("R:1", "R:2", "W:a")).anyTimes();
    // blank names
    expect(status1.getWaitingLocks()).andReturn(List.of("W:3", "W:4")).anyTimes();
    replay(status1);

    // R:+default, R:1
    FateTxnDetails txn1 = new FateTxnDetails(System.currentTimeMillis(), status1, idMap);
    assertNotNull(txn1);
    log.info("T: {}", txn1);

    verify(status1);
  }
}
