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
package org.apache.accumulo.tserver;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.tserver.ScanServer.ScanReservation;
import org.apache.accumulo.tserver.session.ScanSession.TabletResolver;
import org.apache.accumulo.tserver.tablet.SnapshotTablet;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.TabletBase;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

public class ScanServerTest {

  public class TestScanServer extends ScanServer {

    private KeyExtent extent;
    private TabletResolver resolver;
    private ScanReservation reservation;
    private boolean loadTabletFailure = false;

    protected TestScanServer(ScanServerOpts opts, String[] args) {
      super(opts, args);
    }

    @Override
    protected ThriftScanClientHandler newThriftScanClientHandler(WriteTracker writeTracker) {
      return delegate;
    }

    @Override
    protected KeyExtent getKeyExtent(TKeyExtent textent) {
      return extent;
    }

    @Override
    protected TabletResolver getScanTabletResolver(TabletBase tablet) {
      return resolver;
    }

    @Override
    protected TabletResolver
        getBatchScanTabletResolver(final HashMap<KeyExtent,TabletBase> tablets) {
      return resolver;
    }

    @Override
    protected ScanReservation reserveFiles(Collection<KeyExtent> extents)
        throws NotServingTabletException, AccumuloException {
      if (loadTabletFailure) {
        throw new NotServingTabletException();
      }
      return reservation;
    }

    @Override
    protected ScanReservation reserveFiles(long scanId) throws NoSuchScanIDException {
      return reservation;
    }

  }

  private ThriftScanClientHandler handler;

  @Test
  public void testScan() throws Exception {
    handler = createMock(ThriftScanClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    KeyExtent sextent = createMock(KeyExtent.class);
    ScanReservation reservation = createMock(ScanReservation.class);
    SnapshotTablet tablet = createMock(SnapshotTablet.class);
    TRange trange = createMock(TRange.class);
    List<TColumn> tcols = new ArrayList<>();
    List<IterInfo> titer = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();
    List<ByteBuffer> auths = new ArrayList<>();
    TSamplerConfiguration tsc = createMock(TSamplerConfiguration.class);
    String classLoaderContext = new String();
    Map<String,String> execHints = new HashMap<>();
    TabletResolver resolver = createMock(TabletResolver.class);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    expect(reservation.newTablet(ss, sextent)).andReturn(tablet);
    reservation.close();
    reservation.close();
    expect(handler.startScan(tinfo, tcreds, sextent, trange, tcols, 10, titer, ssio, auths, false,
        false, 10, tsc, 30L, classLoaderContext, execHints, resolver, 0L))
        .andReturn(new InitialScan(15, null));
    expect(handler.continueScan(tinfo, 15, 0L)).andReturn(new ScanResult());
    handler.closeScan(tinfo, 15);

    replay(reservation, handler);

    ss.delegate = handler;
    ss.extent = sextent;
    ss.resolver = resolver;
    ss.reservation = reservation;
    ss.clientAddress = HostAndPort.fromParts("127.0.0.1", 1234);

    TKeyExtent textent = createMock(TKeyExtent.class);
    InitialScan is = ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths,
        false, false, 10, tsc, 30L, classLoaderContext, execHints, 0L);
    assertEquals(15, is.getScanID());
    ss.continueScan(tinfo, is.getScanID(), 0L);
    ss.closeScan(tinfo, is.getScanID());
    verify(handler);
  }

  @Test
  public void testTabletLoadFailure() throws Exception {
    handler = createMock(ThriftScanClientHandler.class);

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
        false, 10, tsc, 30L, classLoaderContext, execHints, 0L))
        .andReturn(new InitialScan(15, null));
    expect(handler.continueScan(tinfo, 15, 0L)).andReturn(new ScanResult());
    handler.closeScan(tinfo, 15);

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.delegate = handler;
    ss.loadTabletFailure = true;

    assertThrows(NotServingTabletException.class, () -> {
      ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths, false, false, 10,
          tsc, 30L, classLoaderContext, execHints, 0L);
    });
  }

  @Test
  public void testBatchScan() throws Exception {
    handler = createMock(ThriftScanClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    List<TRange> ranges = new ArrayList<>();
    KeyExtent extent = createMock(KeyExtent.class);
    ScanReservation reservation = createMock(ScanReservation.class);
    SnapshotTablet tablet = createMock(SnapshotTablet.class);
    Map<KeyExtent,List<TRange>> batch = new HashMap<>();
    batch.put(extent, ranges);
    List<TColumn> tcols = new ArrayList<>();
    List<IterInfo> titer = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();
    List<ByteBuffer> auths = new ArrayList<>();
    TSamplerConfiguration tsc = createMock(TSamplerConfiguration.class);
    String classLoaderContext = new String();
    Map<String,String> execHints = new HashMap<>();
    Map<KeyExtent,TabletBase> tablets = new HashMap<>();
    TabletResolver resolver = new TabletResolver() {
      @Override
      public TabletBase getTablet(KeyExtent extent) {
        return tablets.get(extent);
      }

      @Override
      public void close() {}
    };

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    expect(reservation.newTablet(ss, extent)).andReturn(tablet);
    reservation.close();
    reservation.close();
    expect(handler.startMultiScan(tinfo, tcreds, tcols, titer, batch, ssio, auths, false, tsc, 30L,
        classLoaderContext, execHints, resolver, 0L)).andReturn(new InitialMultiScan(15, null));
    expect(handler.continueMultiScan(tinfo, 15, 0L)).andReturn(new MultiScanResult());
    handler.closeMultiScan(tinfo, 15);

    replay(reservation, handler);

    ss.delegate = handler;
    ss.extent = extent;
    ss.resolver = resolver;
    ss.reservation = reservation;
    ss.clientAddress = HostAndPort.fromParts("127.0.0.1", 1234);

    Map<TKeyExtent,List<TRange>> extents = new HashMap<>();
    extents.put(createMock(TKeyExtent.class), ranges);
    InitialMultiScan is = ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths,
        false, tsc, 30L, classLoaderContext, execHints, 0L);
    assertEquals(15, is.getScanID());
    ss.continueMultiScan(tinfo, is.getScanID(), 0L);
    assertEquals(15, is.getScanID());
    ss.closeMultiScan(tinfo, is.getScanID());
    verify(handler);
  }

  @Test
  public void testBatchScanNoRanges() throws Exception {
    handler = createMock(ThriftScanClientHandler.class);

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
    Map<KeyExtent,Tablet> tablets = new HashMap<>();
    TabletResolver resolver = new TabletResolver() {
      @Override
      public Tablet getTablet(KeyExtent extent) {
        return tablets.get(extent);
      }

      @Override
      public void close() {}
    };

    replay(handler);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    ss.delegate = handler;
    ss.resolver = resolver;
    ss.clientAddress = HostAndPort.fromParts("127.0.0.1", 1234);

    assertThrows(TException.class, () -> {
      ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths, false, tsc, 30L,
          classLoaderContext, execHints, 0L);
    });
    verify(handler);
  }

}
