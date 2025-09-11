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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.util.Wait;
import org.apache.accumulo.tserver.ScanServer;
import org.apache.accumulo.tserver.TabletServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

@Tag(MINI_CLUSTER_ONLY)
public class ExitCodesIT extends SharedMiniClusterBase {

  public static enum TerminalBehavior {
    SHUTDOWN, EXCEPTION, ERROR
  };

  public static final String PROXY_METHOD_BEHAVIOR = "proxy.method.behavior";

  public static TerminalBehavior getTerminalBehavior() {
    final String methodBehavior = System.getProperty(PROXY_METHOD_BEHAVIOR);
    Objects.requireNonNull(methodBehavior, methodBehavior + " must exist as an env var");
    return TerminalBehavior.valueOf(methodBehavior);
  }

  public static class ExitCompactor extends Compactor {

    public static void main(String[] args) throws Exception {
      List<String> compactorArgs = new ArrayList<>();
      compactorArgs.add("-o");
      compactorArgs.add(Property.COMPACTOR_GROUP_NAME.getKey() + "=TEST");
      AbstractServer.startServer(new ExitCompactor(getTerminalBehavior(), compactorArgs),
          LoggerFactory.getLogger(ExitCompactor.class));
    }

    private final TerminalBehavior behavior;

    public ExitCompactor(TerminalBehavior behavior, List<String> compactorArgs) {
      super(new ConfigOpts(), compactorArgs.toArray(new String[] {}));
      this.behavior = behavior;
    }

    @Override
    public void updateIdleStatus(boolean isIdle) {
      switch (behavior) {
        case ERROR:
          throw new StackOverflowError("throwing stack overflow error");
        case EXCEPTION:
          throw new RuntimeException("throwing runtime exception");
        case SHUTDOWN:
          requestShutdownForTests();
          break;
        default:
          throw new UnsupportedOperationException(behavior + " is not currently supported");
      }
    }
  }

  public static class ExitScanServer extends ScanServer {

    public static void main(String[] args) throws Exception {
      List<String> sserverArgs = new ArrayList<>();
      sserverArgs.add("-o");
      sserverArgs.add(Property.SSERV_GROUP_NAME.getKey() + "=TEST");
      AbstractServer.startServer(new ExitScanServer(getTerminalBehavior(), sserverArgs),
          LoggerFactory.getLogger(ExitScanServer.class));
    }

    private final TerminalBehavior behavior;

    public ExitScanServer(TerminalBehavior behavior, List<String> sserverArgs) {
      super(new ConfigOpts(), sserverArgs.toArray(new String[] {}));
      this.behavior = behavior;
    }

    @Override
    public void updateIdleStatus(boolean isIdle) {
      switch (behavior) {
        case ERROR:
          throw new StackOverflowError("throwing stack overflow error");
        case EXCEPTION:
          throw new RuntimeException("throwing runtime exception");
        case SHUTDOWN:
          requestShutdownForTests();
          break;
        default:
          throw new UnsupportedOperationException(behavior + " is not currently supported");
      }
    }
  }

  public static class ExitTabletServer extends TabletServer {

    public static void main(String[] args) throws Exception {
      List<String> tserverArgs = new ArrayList<>();
      tserverArgs.add("-o");
      tserverArgs.add(Property.TSERV_GROUP_NAME.getKey() + "=TEST");
      AbstractServer.startServer(new ExitTabletServer(getTerminalBehavior(), tserverArgs),
          LoggerFactory.getLogger(ExitTabletServer.class));
    }

    private final TerminalBehavior behavior;

    public ExitTabletServer(TerminalBehavior behavior, List<String> tserverArgs) {
      super(new ConfigOpts(), ServerContext::new, tserverArgs.toArray(new String[] {}));
      this.behavior = behavior;
    }

    @Override
    public void updateIdleStatus(boolean isIdle) {
      switch (behavior) {
        case ERROR:
          throw new StackOverflowError("throwing stack overflow error");
        case EXCEPTION:
          throw new RuntimeException("throwing runtime exception");
        case SHUTDOWN:
          requestShutdownForTests();
          break;
        default:
          throw new UnsupportedOperationException(behavior + " is not currently supported");
      }
    }
  }

  public static class ExitGC extends SimpleGarbageCollector {

    public static void main(String[] args) throws Exception {
      AbstractServer.startServer(new ExitGC(getTerminalBehavior()),
          LoggerFactory.getLogger(ExitGC.class));
    }

    private final TerminalBehavior behavior;

    public ExitGC(TerminalBehavior behavior) {
      super(new ConfigOpts(), new String[] {});
      this.behavior = behavior;
    }

    @Override
    public void logStats() {
      switch (behavior) {
        case ERROR:
          throw new StackOverflowError("throwing stack overflow error");
        case EXCEPTION:
          throw new RuntimeException("throwing runtime exception");
        case SHUTDOWN:
          requestShutdownForTests();
          break;
        default:
          throw new UnsupportedOperationException(behavior + " is not currently supported");
      }
    }
  }

  public static class ExitManager extends Manager {

    public static void main(String[] args) throws Exception {
      AbstractServer.startServer(new ExitManager(getTerminalBehavior()),
          LoggerFactory.getLogger(ExitManager.class));
    }

    private final TerminalBehavior behavior;

    public ExitManager(TerminalBehavior behavior) throws IOException {
      super(new ConfigOpts(), ServerContext::new, new String[] {});
      this.behavior = behavior;
    }

    @Override
    public void mainWait() throws InterruptedException {
      switch (behavior) {
        case ERROR:
          throw new StackOverflowError("throwing stack overflow error");
        case EXCEPTION:
          throw new RuntimeException("throwing runtime exception");
        case SHUTDOWN:
          requestShutdownForTests();
          break;
        default:
          throw new UnsupportedOperationException(behavior + " is not currently supported");
      }
    }
  }

  @BeforeAll
  public static void beforeTests() throws Exception {
    // Start MiniCluster so that getCluster() does not
    // return null,
    SharedMiniClusterBase.startMiniCluster();
    // Create the resource group or the worker processes will fail
    getCluster().getServerContext().resourceGroupOperations().create(ResourceGroupId.of("TEST"));
    getCluster().getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);
  }

  @AfterAll
  public static void afterTests() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  // Junit doesn't support more than one parameterized
  // argument to a test method. We need to generate
  // the arguments here.
  static Stream<Arguments> generateWorkerProcessArguments() {
    List<Arguments> args = new ArrayList<>();
    for (ServerType st : ServerType.values()) {
      if (st == ServerType.COMPACTOR || st == ServerType.SCAN_SERVER
          || st == ServerType.TABLET_SERVER) {
        for (TerminalBehavior tb : TerminalBehavior.values()) {
          args.add(Arguments.of(st, tb));
        }
      }
    }
    return args.stream();
  }

  @ParameterizedTest
  @MethodSource("generateWorkerProcessArguments")
  public void testWorkerProcesses(ServerType server, TerminalBehavior behavior) throws Exception {
    Map<String,String> properties = new HashMap<>();
    properties.put(PROXY_METHOD_BEHAVIOR, behavior.name());
    getCluster().getConfig().setSystemProperties(properties);
    Class<?> serverClass = switch (server) {
        case COMPACTOR -> ExitCompactor.class;
        case SCAN_SERVER -> ExitScanServer.class;
        case TABLET_SERVER -> ExitTabletServer.class;
        default -> throw new IllegalArgumentException("Unhandled type");
    };
      ProcessInfo pi = getCluster()._exec(serverClass, server, Map.of(), new String[] {});
    Wait.waitFor(() -> !pi.getProcess().isAlive(), 120_000);
    int exitValue = pi.getProcess().exitValue();
    assertEquals(behavior == TerminalBehavior.SHUTDOWN ? 0 : 1, exitValue);
  }

  @ParameterizedTest
  @EnumSource
  public void testGarbageCollector(TerminalBehavior behavior) throws Exception {
    Map<String,String> properties = new HashMap<>();
    properties.put(PROXY_METHOD_BEHAVIOR, behavior.name());
    getCluster().getConfig().setSystemProperties(properties);
    ProcessInfo pi =
        getCluster()._exec(ExitGC.class, ServerType.GARBAGE_COLLECTOR, Map.of(), new String[] {});
    if (behavior == TerminalBehavior.SHUTDOWN) {
      Wait.waitFor(() -> !pi.getProcess().isAlive(), 120_000);
      int exitValue = pi.getProcess().exitValue();
      assertEquals(0, exitValue);
    } else if (behavior == TerminalBehavior.EXCEPTION) {
      // GarbageCollector logs exceptions and keeps going.
      // We need to let this time out and then
      // terminate the process.
      IllegalStateException ise = assertThrows(IllegalStateException.class,
          () -> Wait.waitFor(() -> !pi.getProcess().isAlive(), 60_000));
      assertTrue(ise.getMessage().contains("Timeout exceeded"));
      pi.getProcess().destroyForcibly();
    } else {
      Wait.waitFor(() -> !pi.getProcess().isAlive(), 120_000);
      int exitValue = pi.getProcess().exitValue();
      assertEquals(1, exitValue);
    }
  }

  @ParameterizedTest
  @EnumSource
  public void testManager(TerminalBehavior behavior) throws Exception {
    try {
      getCluster().getClusterControl().stop(ServerType.MANAGER);
      Map<String,String> properties = new HashMap<>();
      properties.put(PROXY_METHOD_BEHAVIOR, behavior.name());
      getCluster().getConfig().setSystemProperties(properties);
      ProcessInfo pi =
          getCluster()._exec(ExitManager.class, ServerType.MANAGER, Map.of(), new String[] {});
      Wait.waitFor(() -> !pi.getProcess().isAlive(), 120_000);
      int exitValue = pi.getProcess().exitValue();
      assertEquals(behavior == TerminalBehavior.SHUTDOWN ? 0 : 1, exitValue);
    } finally {
      getCluster().getClusterControl().stop(ServerType.MANAGER);
    }
  }

}
