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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.functional.ExitCodesIT.ProcessProxy.TerminalBehavior;
import org.apache.accumulo.test.util.Wait;
import org.apache.accumulo.tserver.ScanServer;
import org.apache.accumulo.tserver.TabletServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.ExceptionMethod;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.matcher.ElementMatchers;

@Tag(MINI_CLUSTER_ONLY)
public class ExitCodesIT extends SharedMiniClusterBase {

  public static class ServerContextFunction implements Function<SiteConfiguration,ServerContext> {

    @Override
    public ServerContext apply(SiteConfiguration site) {
      return new ServerContext(site);
    }

  }

  // Dynamic Proxy class for a server instance that will be invoked via
  // Accumulo's Main class using MiniAccumuloClusterImpl._exec(). This
  // ProcessProxy class will determine which class to instantiate
  // and what the terminal behavior should be using two system
  // properties. A subclass of the desired class is dynamically created
  // and started.
  public static class ProcessProxy {

    public static enum TerminalBehavior {
      SHUTDOWN, EXCEPTION, ERROR
    };

    public static final String PROXY_CLASS = "proxy.server";
    public static final String PROXY_METHOD_BEHAVIOR = "proxy.method.behavior";

    public static void main(String[] args) throws Exception {

      final String proxyServer = System.getProperty(PROXY_CLASS);
      Objects.requireNonNull(proxyServer, PROXY_CLASS + " must exist as an env var");
      final String methodBehavior = System.getProperty(PROXY_METHOD_BEHAVIOR);
      Objects.requireNonNull(methodBehavior, methodBehavior + " must exist as an env var");

      final ServerType st = ServerType.valueOf(proxyServer);
      final TerminalBehavior behavior = TerminalBehavior.valueOf(methodBehavior);

      // Determine the constructor arguments and parameters for each server class.
      // Find a method with no-args that does not return anything that is
      // called during the servers run method that we can intercept to signal
      // shutdown, exception, or error.
      final Class<? extends AbstractServer> serverClass;
      final String methodName;
      final Class<?>[] ctorParams;
      final Object[] ctorArgs;
      switch (st) {
        case COMPACTOR:
          List<String> compactorArgs = new ArrayList<>();
          compactorArgs.add("-o");
          compactorArgs.add(Property.COMPACTOR_GROUP_NAME.getKey() + "=TEST");
          serverClass = Compactor.class;
          methodName = "updateIdleStatus";
          ctorParams = new Class<?>[] {ConfigOpts.class, String[].class};
          ctorArgs = new Object[] {new ConfigOpts(), compactorArgs.toArray(new String[] {})};
          break;
        case GARBAGE_COLLECTOR:
          serverClass = SimpleGarbageCollector.class;
          methodName = "logStats";
          ctorParams = new Class<?>[] {ConfigOpts.class, String[].class};
          ctorArgs = new Object[] {new ConfigOpts(), new String[] {}};
          break;
        case MANAGER:
          serverClass = Manager.class;
          methodName = "mainWait";
          ctorParams = new Class<?>[] {ConfigOpts.class, Function.class, String[].class};
          ctorArgs = new Object[] {new ConfigOpts(), new ServerContextFunction(), new String[] {}};
          break;
        case SCAN_SERVER:
          List<String> scanServerArgs = new ArrayList<>();
          scanServerArgs.add("-o");
          scanServerArgs.add(Property.SSERV_GROUP_NAME.getKey() + "=TEST");
          serverClass = ScanServer.class;
          methodName = "updateIdleStatus";
          ctorParams = new Class<?>[] {ConfigOpts.class, String[].class};
          ctorArgs = new Object[] {new ConfigOpts(), scanServerArgs.toArray(new String[] {})};
          break;
        case TABLET_SERVER:
          List<String> tabletServerArgs = new ArrayList<>();
          tabletServerArgs.add("-o");
          tabletServerArgs.add(Property.TSERV_GROUP_NAME.getKey() + "=TEST");
          serverClass = TabletServer.class;
          methodName = "updateIdleStatus";
          ctorParams = new Class<?>[] {ConfigOpts.class, Function.class, String[].class};
          ctorArgs = new Object[] {new ConfigOpts(), new ServerContextFunction(),
              tabletServerArgs.toArray(new String[] {})};
          break;
        case MONITOR:
        case ZOOKEEPER:
        default:
          throw new UnsupportedOperationException(st + " is not currently supported");
      }

      final Implementation implementation;
      switch (behavior) {
        case ERROR:
          implementation =
              ExceptionMethod.throwing(StackOverflowError.class, "throwing unknown error");
          break;
        case EXCEPTION:
          implementation =
              ExceptionMethod.throwing(RuntimeException.class, "throwing runtime exception");
          break;
        case SHUTDOWN:
          implementation =
              MethodCall.invoke(AbstractServer.class.getMethod("requestShutdownForTests"));
          break;
        default:
          throw new UnsupportedOperationException(behavior + " is not currently supported");
      }

      // Dynamically create the subclass with the specified behavior, load it
      // into the JVM, and return it's Class.
      // creates a subclass of server class defining a public constructor that
      // takes an Integer, but calls the server class constructor with the
      // appropriate args. Calls to the intercepted method name are intercepted
      // by the supplied implementation
      final Class<? extends AbstractServer> dynamicServerType =
          new ByteBuddy().subclass(serverClass, ConstructorStrategy.Default.NO_CONSTRUCTORS)
              .name("org.apache.accumulo.test.functional.exit_codes." + serverClass.getSimpleName()
                  + "_" + behavior.name())
              .defineConstructor(Visibility.PUBLIC).withParameters(Integer.class)
              .intercept(MethodCall.invoke(serverClass.getDeclaredConstructor(ctorParams)).onSuper()
                  .with(ctorArgs))
              .method(ElementMatchers.named(methodName)).intercept(implementation).make()
              .load(serverClass.getClassLoader()).getLoaded();

      // Find and invoke the constructor of the dynamically created class
      final Constructor<? extends AbstractServer> ctor =
          dynamicServerType.getDeclaredConstructor(Integer.class);
      final AbstractServer dynamicServerInstance = ctor.newInstance(1);

      // Start the server process
      AbstractServer.startServer(dynamicServerInstance, LoggerFactory.getLogger(serverClass));
    }

  }

  @BeforeAll
  public static void beforeTests() throws Exception {
    // Start MiniCluster so that getCluster() does not
    // return null,
    SharedMiniClusterBase.startMiniCluster();
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
    properties.put(ProcessProxy.PROXY_CLASS, server.name());
    properties.put(ProcessProxy.PROXY_METHOD_BEHAVIOR, behavior.name());
    getCluster().getConfig().setSystemProperties(properties);
    ProcessInfo pi = getCluster()._exec(ProcessProxy.class, server, Map.of(), new String[] {});
    Wait.waitFor(() -> !pi.getProcess().isAlive(), 120_000);
    int exitValue = pi.getProcess().exitValue();
    assertEquals(behavior == TerminalBehavior.SHUTDOWN ? 0 : 1, exitValue);
  }

  static Stream<Arguments> generateGarbageCollectorArguments() {
    List<Arguments> args = new ArrayList<>();
    for (ServerType st : ServerType.values()) {
      if (st == ServerType.GARBAGE_COLLECTOR) {
        for (TerminalBehavior tb : TerminalBehavior.values()) {
          args.add(Arguments.of(st, tb));
        }
      }
    }
    return args.stream();
  }

  @ParameterizedTest
  @MethodSource("generateGarbageCollectorArguments")
  public void testGarbageCollector(ServerType server, TerminalBehavior behavior) throws Exception {
    Map<String,String> properties = new HashMap<>();
    properties.put(ProcessProxy.PROXY_CLASS, server.name());
    properties.put(ProcessProxy.PROXY_METHOD_BEHAVIOR, behavior.name());
    getCluster().getConfig().setSystemProperties(properties);
    ProcessInfo pi = getCluster()._exec(ProcessProxy.class, server, Map.of(), new String[] {});
    if (behavior == TerminalBehavior.SHUTDOWN) {
      Wait.waitFor(() -> !pi.getProcess().isAlive(), 120_000);
      int exitValue = pi.getProcess().exitValue();
      assertEquals(0, exitValue);
    } else if (behavior == TerminalBehavior.EXCEPTION) {
      // GarbageCollector logs exceptions and keeps going.
      // We need to let this time out and then
      // terminate the process.
      IllegalStateException ise = assertThrows(IllegalStateException.class,
          () -> Wait.waitFor(() -> !pi.getProcess().isAlive(), 120_000));
      assertTrue(ise.getMessage().contains("Timeout exceeded"));
      pi.getProcess().destroyForcibly();
    } else {
      Wait.waitFor(() -> !pi.getProcess().isAlive(), 120_000);
      int exitValue = pi.getProcess().exitValue();
      assertEquals(1, exitValue);
    }
  }

  static Stream<Arguments> generateManagerArguments() {
    List<Arguments> args = new ArrayList<>();
    for (ServerType st : ServerType.values()) {
      if (st == ServerType.MANAGER) {
        for (TerminalBehavior tb : TerminalBehavior.values()) {
          args.add(Arguments.of(st, tb));
        }
      }
    }
    return args.stream();
  }

  @ParameterizedTest
  @MethodSource("generateManagerArguments")
  public void testManager(ServerType server, TerminalBehavior behavior) throws Exception {
    try {
      getCluster().getClusterControl().stop(ServerType.MANAGER);
      Map<String,String> properties = new HashMap<>();
      properties.put(ProcessProxy.PROXY_CLASS, server.name());
      properties.put(ProcessProxy.PROXY_METHOD_BEHAVIOR, behavior.name());
      getCluster().getConfig().setSystemProperties(properties);
      ProcessInfo pi = getCluster()._exec(ProcessProxy.class, server, Map.of(), new String[] {});
      Wait.waitFor(() -> !pi.getProcess().isAlive(), 120_000);
      int exitValue = pi.getProcess().exitValue();
      assertEquals(behavior == TerminalBehavior.SHUTDOWN ? 0 : 1, exitValue);
    } finally {
      getCluster().getClusterControl().stop(ServerType.MANAGER);
    }
  }

}
