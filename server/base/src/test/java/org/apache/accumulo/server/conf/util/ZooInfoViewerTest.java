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
package org.apache.accumulo.server.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.Constants.ZINSTANCES;
import static org.apache.accumulo.core.Constants.ZROOT;
import static org.apache.accumulo.core.Constants.ZTABLES;
import static org.apache.accumulo.core.Constants.ZTABLE_NAMESPACE;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.MockServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.PropStoreWatcher;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ZooInfoViewerTest {

  private final Logger log = LoggerFactory.getLogger(ZooInfoViewerTest.class);

  private final VersionedPropCodec propCodec = VersionedPropCodec.getDefault();

  @Test
  public void optionsAllDefault() {
    ZooInfoViewer.Opts opts = new ZooInfoViewer.Opts();
    assertTrue(opts.printAllProps());
    assertTrue(opts.printSysProps());
    assertTrue(opts.printNamespaceProps());
    assertTrue(opts.printTableProps());
  }

  @Test
  public void onlySys() {
    ZooInfoViewer.Opts opts = new ZooInfoViewer.Opts();
    opts.parseArgs(ZooInfoViewer.class.getName(), new String[] {"--system"});

    assertFalse(opts.printAllProps());
    assertTrue(opts.printSysProps());
    assertFalse(opts.printNamespaceProps());
    assertFalse(opts.printTableProps());
  }

  @Test
  public void onlyNamespaces() {
    ZooInfoViewer.Opts opts = new ZooInfoViewer.Opts();
    opts.parseArgs(ZooInfoViewer.class.getName(), new String[] {"-ns", "ns1", "ns2"});

    assertFalse(opts.printAllProps());
    assertFalse(opts.printSysProps());
    assertTrue(opts.printNamespaceProps());
    assertEquals(2, opts.getNamespaces().size());
    assertFalse(opts.printTableProps());
    assertEquals(0, opts.getTables().size());
  }

  @Test
  public void allLongOpts() {
    ZooInfoViewer.Opts opts = new ZooInfoViewer.Opts();
    opts.parseArgs(ZooInfoViewer.class.getName(),
        new String[] {"--system", "--namespaces", "ns1", "ns2", "--tables", "tb1", "tbl2"});

    log.debug("namespaces: {}", opts.getNamespaces());
    log.debug("tables: {}", opts.getTables());

    assertFalse(opts.printAllProps());
    assertTrue(opts.printSysProps());
    assertTrue(opts.printNamespaceProps());
    assertTrue(opts.printTableProps());
    assertEquals(2, opts.getNamespaces().size());
    assertEquals(2, opts.getTables().size());
  }

  @Test
  public void allOpts() {
    ZooInfoViewer.Opts opts = new ZooInfoViewer.Opts();
    opts.parseArgs(ZooInfoViewer.class.getName(), new String[] {"-t", "tb1", "tbl2"});

    assertFalse(opts.printAllProps());
    assertFalse(opts.printSysProps());
    assertFalse(opts.printNamespaceProps());
    assertEquals(0, opts.getNamespaces().size());
    assertTrue(opts.printTableProps());
    assertEquals(2, opts.getTables().size());
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "test generated output")
  @Test
  public void instanceIdOutputTest() throws Exception {
    String uuid = UUID.randomUUID().toString();
    var context = MockServerContext.getWithZK(InstanceId.of(uuid), "fakeHost", 2000);
    ZooReader zooReader = createMock(ZooReader.class);
    expect(context.getZooReader()).andReturn(zooReader).anyTimes();
    var instanceName = "test";
    expect(zooReader.getChildren(eq(ZROOT + ZINSTANCES))).andReturn(List.of(instanceName)).once();
    expect(zooReader.getData(eq(ZROOT + ZINSTANCES + "/" + instanceName)))
        .andReturn(uuid.getBytes(UTF_8)).once();
    replay(context, zooReader);

    String testFileName = "./target/zoo-info-viewer-" + System.currentTimeMillis() + ".txt";

    ZooInfoViewer.Opts opts = new ZooInfoViewer.Opts();
    opts.parseArgs(ZooInfoViewer.class.getName(),
        new String[] {"--print-instances", "--outfile", testFileName});

    ZooInfoViewer viewer = new ZooInfoViewer();
    viewer.generateReport(context, opts);

    verify(context, zooReader);

    String line;
    try (Scanner scanner = new Scanner(new File(testFileName))) {
      boolean found = false;
      while (scanner.hasNext()) {
        line = scanner.nextLine().trim();
        if (line.contains("=")) {
          found = line.startsWith(instanceName) && line.endsWith(uuid);
          break;
        }
      }
      assertTrue(found, "expected instance name, instance id not found");
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "test generated output")
  @Test
  public void instanceNameOutputTest() throws Exception {
    String uuid = UUID.randomUUID().toString();
    var context = MockServerContext.getWithZK(InstanceId.of(uuid), "fakeHost", 2000);
    ZooReader zooReader = createMock(ZooReader.class);
    expect(context.getZooReader()).andReturn(zooReader).anyTimes();
    var instanceName = "test";
    expect(zooReader.getChildren(eq(ZROOT + ZINSTANCES))).andReturn(List.of(instanceName)).once();
    expect(zooReader.getData(eq(ZROOT + ZINSTANCES + "/" + instanceName)))
        .andReturn(uuid.getBytes(UTF_8)).once();
    replay(context, zooReader);

    String testFileName = "./target/zoo-info-viewer-" + System.currentTimeMillis() + ".txt";

    ZooInfoViewer.Opts opts = new ZooInfoViewer.Opts();
    opts.parseArgs(ZooInfoViewer.class.getName(),
        new String[] {"--print-instances", "--outfile", testFileName});

    ZooInfoViewer viewer = new ZooInfoViewer();
    viewer.generateReport(context, opts);

    verify(context, zooReader);

    String line;
    try (Scanner scanner = new Scanner(new File(testFileName))) {
      boolean found = false;
      while (scanner.hasNext()) {
        line = scanner.nextLine();
        if (line.contains("=")) {
          String trimmed = line.trim();
          found = trimmed.startsWith(instanceName) && trimmed.endsWith(uuid);
          break;
        }
      }
      assertTrue(found, "expected instance name, instance id not found");
    }
  }

  @SuppressFBWarnings(value = {"CRLF_INJECTION_LOGS", "PATH_TRAVERSAL_IN"},
      justification = "test generated output")
  @Test
  public void propTest() throws Exception {
    String uuid = UUID.randomUUID().toString();
    InstanceId iid = InstanceId.of(uuid);
    var context = MockServerContext.getWithZK(iid, "fakeHost", 2000);
    ZooReader zooReader = createMock(ZooReader.class);
    expect(context.getZooReader()).andReturn(zooReader).anyTimes();
    var instanceName = "test";
    expect(zooReader.getChildren(eq(ZROOT + ZINSTANCES))).andReturn(List.of(instanceName))
        .anyTimes();
    expect(zooReader.getData(eq(ZROOT + ZINSTANCES + "/" + instanceName)))
        .andReturn(uuid.getBytes(UTF_8)).anyTimes();

    var sysPropBytes = propCodec
        .toBytes(new VersionedProperties(123, Instant.now(), Map.of("s1", "sv1", "s2", "sv2")));
    Capture<Stat> sStat = newCapture();
    expect(zooReader.getData(eq(SystemPropKey.of(iid).getPath()), isA(PropStoreWatcher.class),
        capture(sStat))).andAnswer(() -> {
          Stat s = sStat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion(0); // default version
          s.setDataLength(sysPropBytes.length);
          sStat.setValue(s);
          return sysPropBytes;
        }).once();

    var mockNamespaceIdMap = Map.of(NamespaceId.of("a"), "a_name");
    expect(context.getNamespaceIdToNameMap()).andReturn(mockNamespaceIdMap);

    var nsPropBytes =
        propCodec.toBytes(new VersionedProperties(123, Instant.now(), Map.of("n1", "nv1")));
    NamespaceId nsId = NamespaceId.of("a");
    Capture<Stat> nsStat = newCapture();
    expect(zooReader.getData(eq(NamespacePropKey.of(iid, nsId).getPath()),
        isA(PropStoreWatcher.class), capture(nsStat))).andAnswer(() -> {
          Stat s = nsStat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion(0); // default version
          s.setDataLength(nsPropBytes.length);
          nsStat.setValue(s);
          return nsPropBytes;
        }).once();

    var mockTableIdMap = Map.of(TableId.of("t"), "t_table");
    expect(context.getTableIdToNameMap()).andReturn(mockTableIdMap).once();

    var tBasePath = ZooUtil.getRoot(iid) + ZTABLES;

    var tProps = new VersionedProperties(123, Instant.now(), Map.of("t1", "tv1"));
    var tPropBytes = propCodec.toBytes(tProps);
    TableId tid = TableId.of("t");
    Capture<Stat> stat = newCapture();
    expect(zooReader.getData(eq(TablePropKey.of(iid, tid).getPath()), isA(PropStoreWatcher.class),
        capture(stat))).andAnswer(() -> {
          Stat s = stat.getValue();
          s.setCtime(System.currentTimeMillis());
          s.setMtime(System.currentTimeMillis());
          s.setVersion((int) tProps.getDataVersion());
          s.setDataLength(tPropBytes.length);
          stat.setValue(s);
          return tPropBytes;
        }).once();

    expect(zooReader.getData(tBasePath + "/t" + ZTABLE_NAMESPACE))
        .andReturn("+default".getBytes(UTF_8)).anyTimes();

    replay(context, zooReader);

    NamespacePropKey nsKey = NamespacePropKey.of(iid, nsId);
    log.trace("namespace base path: {}", nsKey.getPath());

    String testFileName = "./target/zoo-info-viewer-" + System.currentTimeMillis() + ".txt";

    ZooInfoViewer.Opts opts = new ZooInfoViewer.Opts();
    opts.parseArgs(ZooInfoViewer.class.getName(),
        new String[] {"--print-props", "--outfile", testFileName});

    ZooInfoViewer viewer = new ZooInfoViewer();
    viewer.generateReport(context, opts);

    verify(context, zooReader);

    Map<String,String> props = new HashMap<>();
    try (Scanner scanner = new Scanner(new File(testFileName))) {
      while (scanner.hasNext()) {
        String line = scanner.nextLine();
        if (line.contains("=")) {
          log.trace("matched line: {}", line);
          String trimmed = line.trim();
          String[] kv = trimmed.split("=");
          props.put(kv[0], kv[1]);
        }
      }
    }
    assertEquals(Map.of("s1", "sv1", "s2", "sv2", "n1", "nv1", "t1", "tv1"), props);
  }

  @SuppressFBWarnings(value = {"CRLF_INJECTION_LOGS", "PATH_TRAVERSAL_IN"},
      justification = "test generated output")
  @Test
  public void idMapTest() throws Exception {
    String uuid = UUID.randomUUID().toString();
    InstanceId iid = InstanceId.of(uuid);

    var mockNamespaceIdMap = Map.of(NamespaceId.of("+accumulo"), "accumulo",
        NamespaceId.of("+default"), "", NamespaceId.of("a_nsid"), "a_namespace_name");
    var mockTableIdMap = Map.of(TableId.of("t_tid"), "t_tablename");
    var context = MockServerContext.get();
    expect(context.getInstanceID()).andReturn(iid).once();
    expect(context.getNamespaceIdToNameMap()).andReturn(mockNamespaceIdMap);
    expect(context.getTableIdToNameMap()).andReturn(mockTableIdMap).once();

    replay(context);

    String testFileName = "./target/zoo-info-viewer-" + System.currentTimeMillis() + ".txt";

    ZooInfoViewer.Opts opts = new ZooInfoViewer.Opts();
    opts.parseArgs(ZooInfoViewer.class.getName(),
        new String[] {"--print-id-map", "--outfile", testFileName});

    ZooInfoViewer viewer = new ZooInfoViewer();
    viewer.generateReport(context, opts);

    verify(context);

    String line;
    Map<String,String> ids = new HashMap<>();
    try (Scanner in = new Scanner(new File(testFileName))) {
      while (in.hasNext()) {
        line = in.nextLine().trim();
        if (line.contains("=>") && !line.contains("ID Mapping")) {
          log.trace("matched line: {}", line);
          String[] kv = line.split("=>");
          ids.put(kv[0].trim(), kv[1].trim());
        }
      }

      log.debug("ids found in output: {}", ids);
      assertEquals(Map.of("+default", "\"\"", "+accumulo", "accumulo", "a_nsid", "a_namespace_name",
          "t_tid", "t_tablename"), ids);
    }
  }
}
