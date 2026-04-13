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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialMultiScan;
import org.apache.accumulo.core.dataImpl.thrift.InitialScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.dataImpl.thrift.ScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletscan.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.tserver.ScanServer.ScanReservation;
import org.apache.accumulo.tserver.session.ScanSession.TabletResolver;
import org.apache.accumulo.tserver.tablet.SnapshotTablet;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.TabletBase;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

public class ScanServerTest {

  public class TestScanServer extends ScanServer {

    private KeyExtent extent;
    private TabletResolver resolver;
    private ScanReservation reservation;
    private ConcurrentHashMap<TableId,TableId> allowedTables;

    protected TestScanServer(ServerOpts opts, String[] args) {
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
    protected ScanReservation reserveFiles(Map<KeyExtent,List<TRange>> extents)
        throws AccumuloException {
      return reservation;
    }

    @Override
    protected ScanReservation reserveFiles(long scanId) throws NoSuchScanIDException {
      return reservation;
    }

    @Override
    ScanReservation reserveFilesInstrumented(Map<KeyExtent,List<TRange>> extents) {
      return reservation;
    }

    @Override
    ScanReservation reserveFilesInstrumented(long scanId) {
      return reservation;
    }

    @Override
    public boolean isShutdownRequested() {
      return false;
    }

    @Override
    protected boolean isAllowed(TCredentials credentials, TableId tid) {
      return allowedTables.containsKey(tid);
    }

    public void addAllowedTable(TableId tid) {
      allowedTables.put(tid, tid);
    }

  }

  private ThriftScanClientHandler handler;

  private static KeyExtent newExtent(TableId tableId) {
    return new KeyExtent(tableId, new Text("m"), new Text("a"));
  }

  @Test
  public void testScan() throws Exception {
    handler = createMock(ThriftScanClientHandler.class);

    TableId tid = TableId.of("1");
    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    KeyExtent sextent = newExtent(tid);
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
    expect(reservation.getFailures()).andReturn(Map.of()).anyTimes();
    reservation.close();
    reservation.close();
    expect(handler.startScan(tinfo, tcreds, sextent, trange, tcols, 10, titer, ssio, auths, false,
        false, 10, tsc, 30L, classLoaderContext, execHints, resolver, 0L))
        .andReturn(new InitialScan(15, null));
    expect(handler.continueScan(tinfo, 15, 0L)).andReturn(new ScanResult());
    handler.closeScan(tinfo, 15);

    replay(reservation, handler);

    ss.allowedTables = new ConcurrentHashMap<>();
    ss.addAllowedTable(tid);
    ss.delegate = handler;
    ss.extent = sextent;
    ss.resolver = resolver;
    ss.reservation = reservation;

    TKeyExtent textent = createMock(TKeyExtent.class);
    InitialScan is = ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths,
        false, false, 10, tsc, 30L, classLoaderContext, execHints, 0L);
    assertEquals(15, is.getScanID());
    ss.continueScan(tinfo, is.getScanID(), 0L);
    ss.closeScan(tinfo, is.getScanID());
    verify(reservation, handler);
  }

  @Test
  public void testScanTabletLoadFailure() throws Exception {
    handler = createMock(ThriftScanClientHandler.class);

    TableId tid = TableId.of("1");
    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    KeyExtent extent = newExtent(tid);
    TKeyExtent textent = extent.toThrift();
    TRange trange = createMock(TRange.class);
    List<TRange> ranges = new ArrayList<>();
    List<TColumn> tcols = new ArrayList<>();
    List<IterInfo> titer = new ArrayList<>();
    Map<String,Map<String,String>> ssio = new HashMap<>();
    List<ByteBuffer> auths = new ArrayList<>();
    TSamplerConfiguration tsc = createMock(TSamplerConfiguration.class);
    String classLoaderContext = new String();
    Map<String,String> execHints = new HashMap<>();
    ScanReservation reservation = createMock(ScanReservation.class);

    TestScanServer ss = partialMockBuilder(TestScanServer.class).createMock();
    expect(reservation.getFailures()).andReturn(Map.of(textent, ranges));
    reservation.close();

    replay(reservation);

    ss.allowedTables = new ConcurrentHashMap<>();
    ss.addAllowedTable(tid);
    ss.extent = extent;
    ss.delegate = handler;
    ss.reservation = reservation;

    assertThrows(NotServingTabletException.class, () -> {
      ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths, false, false, 10,
          tsc, 30L, classLoaderContext, execHints, 0L);
    });

    verify(reservation);
  }

  @Test
  public void testBatchScan() throws Exception {
    handler = createMock(ThriftScanClientHandler.class);

    TableId tid = TableId.of("1");
    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    List<TRange> ranges = new ArrayList<>();
    KeyExtent extent = newExtent(tid);
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
    expect(reservation.getTabletMetadataExtents()).andReturn(Set.of(extent));
    expect(reservation.getFailures()).andReturn(Map.of());
    reservation.close();
    reservation.close();
    expect(handler.startMultiScan(tinfo, tcreds, tcols, titer, batch, ssio, auths, false, tsc, 30L,
        classLoaderContext, execHints, resolver, 0L)).andReturn(new InitialMultiScan(15, null));
    expect(handler.continueMultiScan(tinfo, 15, 0L)).andReturn(new MultiScanResult());
    handler.closeMultiScan(tinfo, 15);

    replay(reservation, handler);

    ss.allowedTables = new ConcurrentHashMap<>();
    ss.addAllowedTable(tid);
    ss.delegate = handler;
    ss.extent = extent;
    ss.resolver = resolver;
    ss.reservation = reservation;

    Map<TKeyExtent,List<TRange>> extents = new HashMap<>();
    extents.put(createMock(TKeyExtent.class), ranges);
    InitialMultiScan is = ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths,
        false, tsc, 30L, classLoaderContext, execHints, 0L);
    assertEquals(15, is.getScanID());
    ss.continueMultiScan(tinfo, is.getScanID(), 0L);
    assertEquals(15, is.getScanID());
    ss.closeMultiScan(tinfo, is.getScanID());
    verify(reservation, handler);
  }

  @Test
  public void testBatchScanTabletLoadFailure() throws Exception {
    handler = createMock(ThriftScanClientHandler.class);

    TableId tid = TableId.of("1");
    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    List<TRange> ranges = new ArrayList<>();
    KeyExtent extent = newExtent(tid);
    TKeyExtent textent = extent.toThrift();
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
    expect(reservation.newTablet(ss, extent)).andReturn(tablet).anyTimes();
    expect(reservation.getTabletMetadataExtents()).andReturn(Set.of());
    expect(reservation.getFailures()).andReturn(Map.of(textent, ranges)).anyTimes();
    reservation.close();
    InitialMultiScan ims = new InitialMultiScan(15, null);
    ims.setResult(new MultiScanResult());
    expect(handler.startMultiScan(tinfo, tcreds, tcols, titer, batch, ssio, auths, false, tsc, 30L,
        classLoaderContext, execHints, resolver, 0L)).andReturn(ims);

    replay(reservation, handler);

    ss.allowedTables = new ConcurrentHashMap<>();
    ss.addAllowedTable(tid);
    ss.delegate = handler;
    ss.extent = extent;
    ss.resolver = resolver;
    ss.reservation = reservation;

    Map<TKeyExtent,List<TRange>> extents = new HashMap<>();
    extents.put(textent, ranges);
    InitialMultiScan is = ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths,
        false, tsc, 30L, classLoaderContext, execHints, 0L);
    assertEquals(15, is.getScanID());
    assertEquals(0, is.getResult().getFailuresSize());

    verify(reservation, handler);

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

    assertThrows(TException.class, () -> {
      ss.startMultiScan(tinfo, tcreds, extents, tcols, titer, ssio, auths, false, tsc, 30L,
          classLoaderContext, execHints, 0L);
    });
    verify(handler);
  }

  @Test
  public void testScanDefaultAllowedTables() throws Exception {
    handler = createMock(ThriftScanClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    KeyExtent sextent = newExtent(SystemTables.METADATA.tableId());
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
    expect(reservation.getFailures()).andReturn(Map.of()).anyTimes();
    reservation.close();
    reservation.close();
    expect(handler.startScan(tinfo, tcreds, sextent, trange, tcols, 10, titer, ssio, auths, false,
        false, 10, tsc, 30L, classLoaderContext, execHints, resolver, 0L))
        .andReturn(new InitialScan(15, null));
    expect(handler.continueScan(tinfo, 15, 0L)).andReturn(new ScanResult());
    handler.closeScan(tinfo, 15);

    replay(reservation, handler);

    ss.allowedTables = new ConcurrentHashMap<>();
    ss.addAllowedTable(SystemTables.METADATA.tableId());
    ss.delegate = handler;
    ss.extent = sextent;
    ss.resolver = resolver;
    ss.reservation = reservation;

    TKeyExtent textent = createMock(TKeyExtent.class);
    InitialScan is = ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths,
        false, false, 10, tsc, 30L, classLoaderContext, execHints, 0L);
    assertEquals(15, is.getScanID());
    ss.continueScan(tinfo, is.getScanID(), 0L);
    ss.closeScan(tinfo, is.getScanID());
    verify(reservation, handler);

  }

  @Test
  public void testScanDisallowedTable() throws Exception {
    handler = createMock(ThriftScanClientHandler.class);

    TInfo tinfo = createMock(TInfo.class);
    TCredentials tcreds = createMock(TCredentials.class);
    KeyExtent sextent = newExtent(SystemTables.METADATA.tableId());
    ScanReservation reservation = createMock(ScanReservation.class);
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
    expect(reservation.getFailures()).andReturn(Map.of()).anyTimes();

    replay(reservation, handler);

    ss.allowedTables = new ConcurrentHashMap<>();
    ss.addAllowedTable(TableId.of("42"));
    ss.delegate = handler;
    ss.extent = sextent;
    ss.resolver = resolver;
    ss.reservation = reservation;

    TKeyExtent textent = createMock(TKeyExtent.class);
    TException te = assertThrows(TException.class, () -> {
      ss.startScan(tinfo, tcreds, textent, trange, tcols, 10, titer, ssio, auths, false, false, 10,
          tsc, 30L, classLoaderContext, execHints, 0L);
    });
    assertTrue(te instanceof TApplicationException);
    TApplicationException tae = (TApplicationException) te;
    assertEquals(TApplicationException.INTERNAL_ERROR, tae.getType());
    assertTrue(tae.getMessage().contains("disallowed by property"));
    verify(reservation, handler);

  }

  @Test
  public void testTableNameRegex() {
    String r = "^(?!accumulo\\.).*$";
    Pattern p = Pattern.compile(r);

    assertFalse(p.matcher("accumulo.root").matches());
    assertFalse(p.matcher("accumulo.metadata").matches());
    assertTrue(p.matcher("test.table").matches());
  }

}
