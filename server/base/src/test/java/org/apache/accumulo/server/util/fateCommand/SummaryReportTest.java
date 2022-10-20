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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.ReadOnlyTStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SummaryReportTest {

  private static final Logger log = LoggerFactory.getLogger(SummaryReportTest.class);

  @Test
  public void blankReport() {
    Map<String,String> idMap = Map.of("1", "ns1", "2", "tbl1");
    FateSummaryReport report = new FateSummaryReport(idMap, null);
    assertNotNull(report);
    assertTrue(report.getReportTime() != 0);
    assertEquals(Map.of(), report.getStatusCounts());
    assertEquals(Map.of(), report.getCmdCounts());
    assertEquals(Map.of(), report.getStepCounts());
    assertEquals(Set.of(), report.getFateDetails());
    assertEquals(Set.of(), report.getStatusFilterNames());
    assertNotNull(report.toJson());
    assertNotNull(report.formatLines());
    log.info("json: {}", report.toJson());
    log.info("formatted: {}", report.formatLines());
  }

  @Test
  public void noTablenameReport() {

    long now = System.currentTimeMillis();

    AdminUtil.TransactionStatus status1 = createMock(AdminUtil.TransactionStatus.class);
    expect(status1.getTimeCreated()).andReturn(now - TimeUnit.DAYS.toMillis(1)).anyTimes();
    expect(status1.getStatus()).andReturn(ReadOnlyTStore.TStatus.IN_PROGRESS).anyTimes();
    expect(status1.getTop()).andReturn(null).anyTimes();
    expect(status1.getTxName()).andReturn(null).anyTimes();
    expect(status1.getTxid()).andReturn("abcdabcd").anyTimes();
    expect(status1.getHeldLocks()).andReturn(List.of()).anyTimes();
    expect(status1.getWaitingLocks()).andReturn(List.of()).anyTimes();

    replay(status1);
    Map<String,String> idMap = Map.of("1", "ns1", "2", "");
    FateSummaryReport report = new FateSummaryReport(idMap, null);
    report.gatherTxnStatus(status1);

    assertNotNull(report);
    assertTrue(report.getReportTime() != 0);
    assertEquals(Map.of("IN_PROGRESS", 1), report.getStatusCounts());
    assertEquals(Map.of("?", 1), report.getCmdCounts());
    assertEquals(Map.of("?", 1), report.getStepCounts());
    assertEquals(Set.of(), report.getStatusFilterNames());
    assertNotNull(report.toJson());
    assertNotNull(report.formatLines());

    assertNotNull(report.getFateDetails());

    log.debug("json: {}", report.toJson());
    log.debug("formatted: {}", report.formatLines());

    verify(status1);
  }
}
