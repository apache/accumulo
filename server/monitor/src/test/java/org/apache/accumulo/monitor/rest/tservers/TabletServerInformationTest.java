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
package org.apache.accumulo.monitor.rest.tservers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;

import org.apache.accumulo.core.master.thrift.BulkImportStatus;
import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.tables.CompactionsTypes;
import org.apache.accumulo.monitor.rest.trace.RecoveryStatusInformation;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class TabletServerInformationTest {

  @Test
  public void testFromThrift() {
    TabletServerStatus ts = new TabletServerStatus();
    ts.setBulkImports(Collections.singletonList(new BulkImportStatus()));
    ts.setDataCacheHits(11);
    ts.setDataCacheRequest(22);
    ts.setFlushs(33);
    ts.setHoldTime(44);
    ts.setIndexCacheHits(55);
    ts.setIndexCacheRequest(66);
    ts.setLastContact(77);
    RecoveryStatus recoveries = new RecoveryStatus();
    recoveries.setName("testRecovery");
    recoveries.setProgress(0.42);
    recoveries.setRuntime(4);
    ts.setLogSorts(Collections.singletonList(recoveries));
    ts.setLookups(88);
    ts.setName("tServerTestName:1234");
    ts.setOsLoad(1.23);
    ts.setResponseTime(99);
    ts.setSyncs(101);
    TableInfo tableInfo = new TableInfo();
    tableInfo.tablets = 202;
    tableInfo.ingestRate = 2.34;
    tableInfo.queryRate = 3.45;
    tableInfo.ingestByteRate = 4.56;
    tableInfo.queryByteRate = 5.67;
    tableInfo.scans = new Compacting(301, 401);
    tableInfo.recs = 502;
    tableInfo.majors = new Compacting(501, 601);
    tableInfo.minors = new Compacting(701, 801);
    ts.setTableMap(Collections.singletonMap("tableId0", tableInfo));
    ts.setVersion("testVersion");

    Monitor monitor = EasyMock.createMock(Monitor.class);

    TabletServerInformation tsi = new TabletServerInformation(monitor, ts);

    assertEquals("tServerTestName:1234", tsi.server);
    assertEquals("tServerTestName:1234", tsi.hostname);
    // can only get within a small distance of time, since it is computed from "now" at time of
    // object creation
    assertTrue(Math.abs((System.currentTimeMillis() - 77) - tsi.lastContact) < 500);
    assertEquals(99, tsi.responseTime);
    assertEquals(1.23, tsi.osload, 0.001);
    assertEquals("testVersion", tsi.version);
    CompactionsTypes compactions = tsi.compactions;
    assertEquals(501, compactions.major.running.intValue());
    assertEquals(601, compactions.major.queued.intValue());
    assertEquals(701, compactions.minor.running.intValue());
    assertEquals(801, compactions.minor.queued.intValue());
    assertEquals(301, compactions.scans.running.intValue());
    assertEquals(401, compactions.scans.queued.intValue());
    assertEquals(202, tsi.tablets);
    assertEquals(2.34, tsi.ingest, 0.001);
    assertEquals(3.45, tsi.query, 0.001);
    assertEquals(4.56, tsi.ingestMB, 0.001);
    assertEquals(5.67, tsi.queryMB, 0.001);
    assertEquals(301, tsi.scans.intValue());
    assertEquals(0.0, tsi.scansessions, 0.001); // can't test here; this comes from
                                                // ManagerMonitorInfo
    assertEquals(tsi.scansessions, tsi.scanssessions, 0.001);
    assertEquals(44, tsi.holdtime);
    assertEquals("tServerTestName:1234", tsi.ip);
    assertEquals(502, tsi.entries);
    assertEquals(88, tsi.lookups);
    assertEquals(55, tsi.indexCacheHits);
    assertEquals(66, tsi.indexCacheRequests);
    assertEquals(11, tsi.dataCacheHits);
    assertEquals(22, tsi.dataCacheRequests);
    assertEquals(55 / 66.0, tsi.indexCacheHitRate, 0.001);
    assertEquals(11 / 22.0, tsi.dataCacheHitRate, 0.001);
    RecoveryStatusInformation rec = tsi.logRecoveries.get(0);
    assertEquals("testRecovery", rec.name);
    assertEquals(0.42, rec.progress, 0.001);
    assertEquals(4, rec.runtime.intValue());
  }

}
