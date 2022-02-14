/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialMultiScan;
import org.apache.accumulo.core.dataImpl.thrift.InitialScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.dataImpl.thrift.ScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.thrift.TException;
import org.junit.Test;

public class ScanServerTest {

  public class TestScanServer extends ScanServer {

    private boolean loadTablet;
    private KeyExtent extent;

    protected TestScanServer(ServerOpts opts, String[] args) {
      super(opts, args);
    }

    @Override
    protected ThriftClientHandler getHandler() {
      return handler;
    }

    @Override
    protected ScanInformation loadTablet(KeyExtent textent)
        throws IllegalArgumentException, IOException, AccumuloException {
      if (loadTablet) {
        ScanInformation si = new ScanInformation();
        si.setTablet(createNiceMock(Tablet.class));
        si.setExtent(createNiceMock(KeyExtent.class));
        return si;
      }
      return null;
    }

    @Override
    protected void endScan(ScanInformation si) {
      scans.remove(si.getScanId(), si);
    }

    @Override
    protected void logOnlineTablets() {}

  }

  private ThriftClientHandler handler;

  @Test
  public void testScan() throws Exception {
    handler = createMock(ThriftClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    KeyExtent sextent = createMock(KeyExtent.class);
    TRange trange = createMock(TRange.class);
    List<TColumn> tcols = new ArrayList<>();
    List<IterInfo> titer = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();
    List<ByteBuffer> auths = new ArrayList<>();
    TSamplerConfiguration tsc = createMock(TSamplerConfiguration.class);
    String classLoaderContext = new String();
    Map<String,String> execHints = new HashMap<>();

    expect(handler.startScan(tinfo, tcreds, sextent, trange, tcols, 10, titer, ssio, auths, false,
        false, 10, tsc, 30L, classLoaderContext, execHints, ke->null)).andReturn(new InitialScan(15, null));
    expect(handler.continueScan(tinfo, 15)).andReturn(new ScanResult());
    handler.closeScan(tinfo, 15);

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.maxConcurrentScans = 1;
    ss.scans = new ConcurrentHashMap<>(1, 1.0f, 1);
    ss.loadTablet = true;
    ss.handler = handler;
    ss.extent = sextent;

    TKeyExtent textent = createMock(TKeyExtent.class);
    assertEquals(0, ss.scans.size());
    InitialScan is = ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths,
        false, false, 10, tsc, 30L, classLoaderContext, execHints);
    assertEquals(15, is.getScanID());
    assertEquals(1, ss.scans.size());
    assertNotNull(ss.scans.get(15L).getExtent());
    assertNotNull(ss.scans.get(15L).getTablet());
    assertEquals((Long) 15L, ss.scans.get(15L).getScanId());
    ss.continueScan(tinfo, is.getScanID());
    assertEquals(1, ss.scans.size());
    assertNotNull(ss.scans.get(15L).getExtent());
    assertNotNull(ss.scans.get(15L).getTablet());
    assertEquals((Long) 15L, ss.scans.get(15L).getScanId());
    ss.closeScan(tinfo, is.getScanID());
    assertEquals(0, ss.scans.size());
    verify(handler);
  }

  @Test
  public void testScanInUse() throws Exception {
    handler = createMock(ThriftClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    KeyExtent sextent = createMock(KeyExtent.class);
    TRange trange = createMock(TRange.class);
    List<TColumn> tcols = new ArrayList<>();
    List<IterInfo> titer = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();
    List<ByteBuffer> auths = new ArrayList<>();
    TSamplerConfiguration tsc = createMock(TSamplerConfiguration.class);
    String classLoaderContext = new String();
    Map<String,String> execHints = new HashMap<>();

    expect(handler.startScan(tinfo, tcreds, sextent, trange, tcols, 10, titer, ssio, auths, false,
        false, 10, tsc, 30L, classLoaderContext, execHints, ke->null)).andReturn(new InitialScan(15, null));
    expect(handler.continueScan(tinfo, 15)).andReturn(new ScanResult());
    handler.closeScan(tinfo, 15);

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.maxConcurrentScans = 1;
    ss.scans = new ConcurrentHashMap<>(1, 1.0f, 1);
    ss.loadTablet = true;
    ss.handler = handler;
    ss.extent = sextent;

    TKeyExtent textent = createMock(TKeyExtent.class);
    assertEquals(0, ss.scans.size());
    InitialScan is = ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths,
        false, false, 10, tsc, 30L, classLoaderContext, execHints);
    assertEquals(15, is.getScanID());
    assertEquals(1, ss.scans.size());
    assertNotNull(ss.scans.get(15L).getExtent());
    assertNotNull(ss.scans.get(15L).getTablet());
    assertEquals((Long) 15L, ss.scans.get(15L).getScanId());
    ss.continueScan(tinfo, is.getScanID());
    assertEquals(1, ss.scans.size());
    assertNotNull(ss.scans.get(15L).getExtent());
    assertNotNull(ss.scans.get(15L).getTablet());
    assertEquals((Long) 15L, ss.scans.get(15L).getScanId());
    assertThrows(TException.class, () -> {
      ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths, false, false, 10,
          tsc, 30L, classLoaderContext, execHints);
    });
  }

  @Test
  public void testTabletLoadFailure() throws Exception {
    handler = createMock(ThriftClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    TKeyExtent textent = createMock(TKeyExtent.class);
    TRange trange = createMock(TRange.class);
    List<TColumn> tcols = new ArrayList<>();
    List<IterInfo> titer = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();
    List<ByteBuffer> auths = new ArrayList<>();
    TSamplerConfiguration tsc = createMock(TSamplerConfiguration.class);
    String classLoaderContext = new String();
    Map<String,String> execHints = new HashMap<>();

    expect(handler.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths, false,
        false, 10, tsc, 30L, classLoaderContext, execHints)).andReturn(new InitialScan(15, null));
    expect(handler.continueScan(tinfo, 15)).andReturn(new ScanResult());
    handler.closeScan(tinfo, 15);

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.maxConcurrentScans = 1;
    ss.scans = new ConcurrentHashMap<>(1, 1.0f, 1);
    ss.loadTablet = false;
    ss.handler = handler;

    assertEquals(0, ss.scans.size());
    assertThrows(NotServingTabletException.class, () -> {
      ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths, false, false, 10,
          tsc, 30L, classLoaderContext, execHints);
    });
    assertEquals(0, ss.scans.size());
  }

  @Test
  public void testBatchScan() throws Exception {
    handler = createMock(ThriftClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    List<TRange> ranges = new ArrayList<>();
    KeyExtent sextent = createMock(KeyExtent.class);
    Map<KeyExtent,List<TRange>> sextents = new HashMap<>();
    sextents.put(sextent, ranges);
    List<TColumn> tcols = new ArrayList<>();
    List<IterInfo> titer = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();
    List<ByteBuffer> auths = new ArrayList<>();
    TSamplerConfiguration tsc = createMock(TSamplerConfiguration.class);
    String classLoaderContext = new String();
    Map<String,String> execHints = new HashMap<>();

    expect(handler.startMultiScan(tinfo, tcreds, tcols, titer, sextents, ssio, auths, false, tsc,
        30L, classLoaderContext, execHints, ke->null)).andReturn(new InitialMultiScan(15, null));
    expect(handler.continueMultiScan(tinfo, 15)).andReturn(new MultiScanResult());
    handler.closeMultiScan(tinfo, 15);

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.maxConcurrentScans = 1;
    ss.scans = new ConcurrentHashMap<>(1, 1.0f, 1);
    ss.loadTablet = true;
    ss.handler = handler;
    ss.extent = sextent;

    Map<TKeyExtent,List<TRange>> extents = new HashMap<>();
    extents.put(createMock(TKeyExtent.class), ranges);
    assertEquals(0, ss.scans.size());
    InitialMultiScan is = ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths,
        false, tsc, 30L, classLoaderContext, execHints);
    assertEquals(15, is.getScanID());
    assertEquals(1, ss.scans.size());
    assertNotNull(ss.scans.get(15L).getExtent());
    assertNotNull(ss.scans.get(15L).getTablet());
    assertEquals((Long) 15L, ss.scans.get(15L).getScanId());
    ss.continueMultiScan(tinfo, is.getScanID());
    assertEquals(15, is.getScanID());
    assertEquals(1, ss.scans.size());
    assertNotNull(ss.scans.get(15L).getExtent());
    assertNotNull(ss.scans.get(15L).getTablet());
    assertEquals((Long) 15L, ss.scans.get(15L).getScanId());
    ss.closeMultiScan(tinfo, is.getScanID());
    assertEquals(0, ss.scans.size());
    verify(handler);
  }

  @Test
  public void testBatchScanInUse() throws Exception {
    handler = createMock(ThriftClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    List<TRange> ranges = new ArrayList<>();
    KeyExtent sextent = createMock(KeyExtent.class);
    Map<KeyExtent,List<TRange>> sextents = new HashMap<>();
    sextents.put(sextent, ranges);
    List<TColumn> tcols = new ArrayList<>();
    List<IterInfo> titer = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();
    List<ByteBuffer> auths = new ArrayList<>();
    TSamplerConfiguration tsc = createMock(TSamplerConfiguration.class);
    String classLoaderContext = new String();
    Map<String,String> execHints = new HashMap<>();

    expect(handler.startMultiScan(tinfo, tcreds, tcols, titer, sextents, ssio, auths, false, tsc,
        30L, classLoaderContext, execHints, ke->null)).andReturn(new InitialMultiScan(15, null));
    expect(handler.continueMultiScan(tinfo, 15)).andReturn(new MultiScanResult());
    handler.closeMultiScan(tinfo, 15);

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.maxConcurrentScans = 1;
    ss.scans = new ConcurrentHashMap<>(1, 1.0f, 1);
    ss.loadTablet = true;
    ss.handler = handler;
    ss.extent = sextent;

    Map<TKeyExtent,List<TRange>> extents = new HashMap<>();
    extents.put(createMock(TKeyExtent.class), ranges);
    assertEquals(0, ss.scans.size());
    InitialMultiScan is = ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths,
        false, tsc, 30L, classLoaderContext, execHints);
    assertEquals(15, is.getScanID());
    assertEquals(1, ss.scans.size());
    assertNotNull(ss.scans.get(15L).getExtent());
    assertNotNull(ss.scans.get(15L).getTablet());
    assertEquals((Long) 15L, ss.scans.get(15L).getScanId());
    ss.continueMultiScan(tinfo, is.getScanID());
    assertEquals(15, is.getScanID());
    assertEquals(1, ss.scans.size());
    assertNotNull(ss.scans.get(15L).getExtent());
    assertNotNull(ss.scans.get(15L).getTablet());
    assertEquals((Long) 15L, ss.scans.get(15L).getScanId());
    assertThrows(TException.class, () -> {
      ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths, false, tsc, 30L,
          classLoaderContext, execHints);
    });
  }

  @Test
  public void testBatchScanNoRanges() throws Exception {
    handler = createMock(ThriftClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    Map<TKeyExtent,List<TRange>> extents = new HashMap<>();
    List<TColumn> tcols = new ArrayList<>();
    List<IterInfo> titer = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();
    List<ByteBuffer> auths = new ArrayList<>();
    TSamplerConfiguration tsc = createMock(TSamplerConfiguration.class);
    String classLoaderContext = new String();
    Map<String,String> execHints = new HashMap<>();

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.maxConcurrentScans = 1;
    ss.scans = new ConcurrentHashMap<>(1, 1.0f, 1);
    ss.loadTablet = true;
    ss.handler = handler;

    assertEquals(0, ss.scans.size());
    assertThrows(TException.class, () -> {
      ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths, false, tsc, 30L,
          classLoaderContext, execHints);
    });
    assertEquals(0, ss.scans.size());
    verify(handler);
  }

  @Test
  public void testBatchScanTooManyRanges() throws Exception {
    handler = createMock(ThriftClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    List<TRange> ranges = new ArrayList<>();
    Map<TKeyExtent,List<TRange>> extents = new HashMap<>();
    extents.put(createMock(TKeyExtent.class), ranges);
    extents.put(createMock(TKeyExtent.class), ranges);
    List<TColumn> tcols = new ArrayList<>();
    List<IterInfo> titer = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();
    List<ByteBuffer> auths = new ArrayList<>();
    TSamplerConfiguration tsc = createMock(TSamplerConfiguration.class);
    String classLoaderContext = new String();
    Map<String,String> execHints = new HashMap<>();

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.maxConcurrentScans = 1;
    ss.scans = new ConcurrentHashMap<>(1, 1.0f, 1);
    ss.loadTablet = true;
    ss.handler = handler;

    assertEquals(0, ss.scans.size());
    assertThrows(TException.class, () -> {
      ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths, false, tsc, 30L,
          classLoaderContext, execHints);
    });
    assertEquals(0, ss.scans.size());
    verify(handler);
  }

}
