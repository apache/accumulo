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
package org.apache.accumulo.test.start;

import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.compactor.CompactorExecutable;
import org.apache.accumulo.coordinator.CoordinatorExecutable;
import org.apache.accumulo.core.file.rfile.CreateEmpty;
import org.apache.accumulo.core.file.rfile.GenerateSplits;
import org.apache.accumulo.core.file.rfile.PrintInfo;
import org.apache.accumulo.core.file.rfile.SplitLarge;
import org.apache.accumulo.core.util.CreateToken;
import org.apache.accumulo.core.util.Help;
import org.apache.accumulo.core.util.Version;
import org.apache.accumulo.gc.GCExecutable;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.manager.ManagerExecutable;
import org.apache.accumulo.minicluster.MiniAccumuloRunner;
import org.apache.accumulo.miniclusterImpl.MiniClusterExecutable;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.MonitorExecutable;
import org.apache.accumulo.server.conf.CheckCompactionConfig;
import org.apache.accumulo.server.conf.CheckServerConfig;
import org.apache.accumulo.server.conf.util.ZooInfoViewer;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.ConvertConfig;
import org.apache.accumulo.server.util.DumpZookeeper;
import org.apache.accumulo.server.util.ECAdmin;
import org.apache.accumulo.server.util.Info;
import org.apache.accumulo.server.util.LoginProperties;
import org.apache.accumulo.server.util.ZooKeeperMain;
import org.apache.accumulo.server.util.ZooZap;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.start.Main;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.accumulo.tserver.ScanServerExecutable;
import org.apache.accumulo.tserver.TServerExecutable;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.logger.LogReader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(SUNNY_DAY)
public class KeywordStartIT {

  private final Logger log = LoggerFactory.getLogger(getClass());

  @Test
  public void testKeywordsMatch() {
    for (Entry<String,KeywordExecutable> entry : Main.getExecutables(getClass().getClassLoader())
        .entrySet()) {
      assertEquals(entry.getKey(), entry.getValue().keyword());
    }
  }

  @Test
  public void testCheckDuplicates() {
    NoOp one = new NoOp("one");
    NoOp anotherOne = new NoOp("another");
    NoOp two = new NoOp("two");
    NoOp three = new NoOp("three");
    List<NoOp> services = Arrays.asList(one, three, two, two, three, three, anotherOne);
    assertEquals(7, services.size());
    Map<String,KeywordExecutable> results = Main.checkDuplicates(services);
    assertTrue(results.containsKey(one.keyword()));
    assertTrue(results.containsKey(anotherOne.keyword()));
    assertFalse(results.containsKey(two.keyword()));
    assertFalse(results.containsKey(three.keyword()));
    assertEquals(2, results.size());
  }

  /**
   * This test guards against accidental renaming or incorrect naming of the keyword used to
   * identify the service. The keyword is used to access the commands via the command line, so
   * changes are visible to users and should not be changed.
   * <p>
   * Note: this test may fail in Eclipse, if the services files haven't been generated by the
   * AutoService annotation processor
   */
  @Test
  public void testExpectedClasses() {
    assumeTrue(new File(System.getProperty("user.dir") + "/src").exists());
    TreeMap<String,Class<? extends KeywordExecutable>> expectSet = new TreeMap<>();
    expectSet.put("admin", Admin.class);
    expectSet.put("check-compaction-config", CheckCompactionConfig.class);
    expectSet.put("check-server-config", CheckServerConfig.class);
    expectSet.put("compaction-coordinator", CoordinatorExecutable.class);
    expectSet.put("compactor", CompactorExecutable.class);
    expectSet.put("convert-config", ConvertConfig.class);
    expectSet.put("create-token", CreateToken.class);
    expectSet.put("dump-zoo", DumpZookeeper.class);
    expectSet.put("ec-admin", ECAdmin.class);
    expectSet.put("gc", GCExecutable.class);
    expectSet.put("generate-splits", GenerateSplits.class);
    expectSet.put("help", Help.class);
    expectSet.put("info", Info.class);
    expectSet.put("init", Initialize.class);
    expectSet.put("login-info", LoginProperties.class);
    expectSet.put("manager", ManagerExecutable.class);
    expectSet.put("minicluster", MiniClusterExecutable.class);
    expectSet.put("monitor", MonitorExecutable.class);
    expectSet.put("rfile-info", PrintInfo.class);
    expectSet.put("wal-info", LogReader.class);
    expectSet.put("shell", Shell.class);
    expectSet.put("tserver", TServerExecutable.class);
    expectSet.put("version", Version.class);
    expectSet.put("zookeeper", ZooKeeperMain.class);
    expectSet.put("create-empty", CreateEmpty.class);
    expectSet.put("split-large", SplitLarge.class);
    expectSet.put("sserver", ScanServerExecutable.class);
    expectSet.put("zoo-info-viewer", ZooInfoViewer.class);
    expectSet.put("zoo-zap", ZooZap.class);

    Iterator<Entry<String,Class<? extends KeywordExecutable>>> expectIter =
        expectSet.entrySet().iterator();
    TreeMap<String,KeywordExecutable> actualSet =
        new TreeMap<>(Main.getExecutables(getClass().getClassLoader()));
    Iterator<Entry<String,KeywordExecutable>> actualIter = actualSet.entrySet().iterator();
    Entry<String,Class<? extends KeywordExecutable>> expected;
    Entry<String,KeywordExecutable> actual;
    while (expectIter.hasNext() && actualIter.hasNext()) {
      expected = expectIter.next();
      actual = actualIter.next();
      assertEquals(expected.getKey(), actual.getKey());
      assertEquals(expected.getValue(), actual.getValue().getClass());
    }
    boolean moreExpected = expectIter.hasNext();
    if (moreExpected) {
      while (expectIter.hasNext()) {
        log.warn("Missing class for keyword '{}'", expectIter.next());
      }
    }
    assertFalse(moreExpected, "Missing expected classes");
    boolean moreActual = actualIter.hasNext();
    if (moreActual) {
      while (actualIter.hasNext()) {
        log.warn("Extra class found with keyword '{}'", actualIter.next());
      }
    }
    assertFalse(moreActual, "Found additional unexpected classes");
  }

  @Test
  public void checkHasMain() {
    assertFalse(hasMain(this.getClass()),
        "Sanity check for test failed. Somehow the test class has a main method");

    HashSet<Class<?>> expectSet = new HashSet<>();
    expectSet.add(Admin.class);
    expectSet.add(CheckCompactionConfig.class);
    expectSet.add(CreateToken.class);
    expectSet.add(DumpZookeeper.class);
    expectSet.add(Info.class);
    expectSet.add(Initialize.class);
    expectSet.add(LoginProperties.class);
    expectSet.add(MiniAccumuloRunner.class);
    expectSet.add(Monitor.class);
    expectSet.add(PrintInfo.class);
    expectSet.add(LogReader.class);
    expectSet.add(Shell.class);
    expectSet.add(SimpleGarbageCollector.class);
    expectSet.add(TabletServer.class);
    expectSet.add(ZooKeeperMain.class);

    for (Class<?> c : expectSet) {
      assertTrue(hasMain(c), "Class " + c.getName() + " is missing a main method!");
    }

  }

  private static boolean hasMain(Class<?> classToCheck) {
    Method main;
    try {
      main = classToCheck.getMethod("main", String[].class);
    } catch (NoSuchMethodException e) {
      return false;
    }
    return Modifier.isPublic(main.getModifiers()) && Modifier.isStatic(main.getModifiers());
  }

  private static class NoOp implements KeywordExecutable {

    private final String kw;

    public NoOp(String kw) {
      this.kw = kw;
    }

    @Override
    public String keyword() {
      return kw;
    }

    @Override
    public String description() {
      return kw;
    }

    @Override
    public void execute(String[] args) {}

  }
}
