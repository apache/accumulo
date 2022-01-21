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
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.tserver.ScanServer.CurrentScan;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.junit.Test;

public class ScanServerTest {

  public class TestScanServer extends ScanServer {

    private boolean loadTablet;

    protected TestScanServer(ServerOpts opts, String[] args) {
      super(opts, args);
    }

    @Override
    protected ThriftClientHandler getHandler() {
      return handler;
    }

    @Override
    protected synchronized boolean loadTablet(TKeyExtent textent)
        throws IllegalArgumentException, IOException, AccumuloException {
      if (loadTablet) {
        currentScan.tablet = createNiceMock(Tablet.class);
        currentScan.extent = createNiceMock(KeyExtent.class);
        return true;
      }
      return false;
    }

    @Override
    protected synchronized void endScan() {
      currentScan.extent = null;
      currentScan.tablet = null;
      currentScan.scanId = null;
    }
  }

  private ThriftClientHandler handler;

  @Test
  public void testScan() throws Exception {
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
    ss.currentScan = new CurrentScan();
    ss.loadTablet = true;
    ss.handler = handler;

    assertNull(ss.currentScan.extent);
    assertNull(ss.currentScan.tablet);
    assertNull(ss.currentScan.scanId);
    InitialScan is = ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths,
        false, false, 10, tsc, 30L, classLoaderContext, execHints);
    assertEquals(15, is.getScanID());
    assertNotNull(ss.currentScan.extent);
    assertNotNull(ss.currentScan.tablet);
    assertNotNull(ss.currentScan.scanId);
    ss.continueScan(tinfo, is.getScanID());
    assertNotNull(ss.currentScan.extent);
    assertNotNull(ss.currentScan.tablet);
    assertNotNull(ss.currentScan.scanId);
    ss.closeScan(tinfo, is.getScanID());
    assertNull(ss.currentScan.extent);
    assertNull(ss.currentScan.tablet);
    assertNull(ss.currentScan.scanId);
    verify(handler);
  }

  @Test(expected = RuntimeException.class)
  public void testScanInUse() throws Exception {
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
    ss.currentScan = new CurrentScan();
    ss.loadTablet = true;
    ss.handler = handler;

    assertNull(ss.currentScan.extent);
    assertNull(ss.currentScan.tablet);
    assertNull(ss.currentScan.scanId);
    InitialScan is = ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths,
        false, false, 10, tsc, 30L, classLoaderContext, execHints);
    assertEquals(15, is.getScanID());
    assertNotNull(ss.currentScan.extent);
    assertNotNull(ss.currentScan.tablet);
    assertNotNull(ss.currentScan.scanId);
    ss.continueScan(tinfo, is.getScanID());
    assertNotNull(ss.currentScan.extent);
    assertNotNull(ss.currentScan.tablet);
    assertNotNull(ss.currentScan.scanId);
    ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths, false, false, 10,
        tsc, 30L, classLoaderContext, execHints);
  }

  @Test(expected = RuntimeException.class)
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
    ss.currentScan = new CurrentScan();
    ss.loadTablet = false;
    ss.handler = handler;

    assertNull(ss.currentScan.extent);
    assertNull(ss.currentScan.tablet);
    assertNull(ss.currentScan.scanId);
    ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths, false, false, 10,
        tsc, 30L, classLoaderContext, execHints);
    assertNull(ss.currentScan.extent);
    assertNull(ss.currentScan.tablet);
    assertNull(ss.currentScan.scanId);
  }

  @Test
  public void testBatchScan() throws Exception {
    handler = createMock(ThriftClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    List<TRange> ranges = new ArrayList<>();
    Map<TKeyExtent,List<TRange>> extents = new HashMap<>();
    extents.put(createMock(TKeyExtent.class), ranges);
    List<TColumn> tcols = new ArrayList<>();
    List<IterInfo> titer = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();
    List<ByteBuffer> auths = new ArrayList<>();
    TSamplerConfiguration tsc = createMock(TSamplerConfiguration.class);
    String classLoaderContext = new String();
    Map<String,String> execHints = new HashMap<>();

    expect(handler.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths, false, tsc,
        30L, classLoaderContext, execHints)).andReturn(new InitialMultiScan(15, null));
    expect(handler.continueMultiScan(tinfo, 15)).andReturn(new MultiScanResult());
    handler.closeMultiScan(tinfo, 15);

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.currentScan = new CurrentScan();
    ss.loadTablet = true;
    ss.handler = handler;

    assertNull(ss.currentScan.extent);
    assertNull(ss.currentScan.tablet);
    assertNull(ss.currentScan.scanId);
    InitialMultiScan is = ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths,
        false, tsc, 30L, classLoaderContext, execHints);
    assertNotNull(ss.currentScan.extent);
    assertNotNull(ss.currentScan.tablet);
    assertNotNull(ss.currentScan.scanId);
    assertEquals(15, is.getScanID());
    ss.continueMultiScan(tinfo, is.getScanID());
    assertNotNull(ss.currentScan.extent);
    assertNotNull(ss.currentScan.tablet);
    assertNotNull(ss.currentScan.scanId);
    ss.closeMultiScan(tinfo, is.getScanID());
    assertNull(ss.currentScan.extent);
    assertNull(ss.currentScan.tablet);
    assertNull(ss.currentScan.scanId);
    verify(handler);
  }

  @Test(expected = RuntimeException.class)
  public void testBatchScanInUse() throws Exception {
    handler = createMock(ThriftClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    List<TRange> ranges = new ArrayList<>();
    Map<TKeyExtent,List<TRange>> extents = new HashMap<>();
    extents.put(createMock(TKeyExtent.class), ranges);
    List<TColumn> tcols = new ArrayList<>();
    List<IterInfo> titer = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();
    List<ByteBuffer> auths = new ArrayList<>();
    TSamplerConfiguration tsc = createMock(TSamplerConfiguration.class);
    String classLoaderContext = new String();
    Map<String,String> execHints = new HashMap<>();

    expect(handler.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths, false, tsc,
        30L, classLoaderContext, execHints)).andReturn(new InitialMultiScan(15, null));
    expect(handler.continueMultiScan(tinfo, 15)).andReturn(new MultiScanResult());
    handler.closeMultiScan(tinfo, 15);

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.currentScan = new CurrentScan();
    ss.loadTablet = true;
    ss.handler = handler;

    assertNull(ss.currentScan.extent);
    assertNull(ss.currentScan.tablet);
    assertNull(ss.currentScan.scanId);
    InitialMultiScan is = ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths,
        false, tsc, 30L, classLoaderContext, execHints);
    assertNotNull(ss.currentScan.extent);
    assertNotNull(ss.currentScan.tablet);
    assertNotNull(ss.currentScan.scanId);
    assertEquals(15, is.getScanID());
    ss.continueMultiScan(tinfo, is.getScanID());
    assertNotNull(ss.currentScan.extent);
    assertNotNull(ss.currentScan.tablet);
    assertNotNull(ss.currentScan.scanId);
    ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths, false, tsc, 30L,
        classLoaderContext, execHints);
  }

  @Test(expected = RuntimeException.class)
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

    expect(handler.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths, false, tsc,
        30L, classLoaderContext, execHints)).andReturn(new InitialMultiScan(15, null));
    expect(handler.continueScan(tinfo, 15)).andReturn(new ScanResult());
    handler.closeScan(tinfo, 15);

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.currentScan = new CurrentScan();
    ss.loadTablet = true;
    ss.handler = handler;

    assertNull(ss.currentScan.extent);
    assertNull(ss.currentScan.tablet);
    assertNull(ss.currentScan.scanId);
    ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths, false, tsc, 30L,
        classLoaderContext, execHints);
    assertNull(ss.currentScan.extent);
    assertNull(ss.currentScan.tablet);
    assertNull(ss.currentScan.scanId);
    verify(handler);
  }

  @Test(expected = RuntimeException.class)
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

    expect(handler.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths, false, tsc,
        30L, classLoaderContext, execHints)).andReturn(new InitialMultiScan(15, null));
    expect(handler.continueScan(tinfo, 15)).andReturn(new ScanResult());
    handler.closeScan(tinfo, 15);

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.currentScan = new CurrentScan();
    ss.loadTablet = true;
    ss.handler = handler;

    assertNull(ss.currentScan.extent);
    assertNull(ss.currentScan.tablet);
    assertNull(ss.currentScan.scanId);
    ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths, false, tsc, 30L,
        classLoaderContext, execHints);
    assertNull(ss.currentScan.extent);
    assertNull(ss.currentScan.tablet);
    assertNull(ss.currentScan.scanId);
    verify(handler);
  }

}
