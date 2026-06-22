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
package org.apache.accumulo.monitor;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Singleton;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockSupport.HAServiceLockWatcher;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.monitor.next.InformationFetcher;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.ee10.servlet.ResourceServlet;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.ConnectionStatistics;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.mvc.MvcFeature;
import org.glassfish.jersey.server.mvc.freemarker.FreemarkerMvcFeature;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

/**
 * Serve manager statistics with an embedded web server.
 */
public class Monitor extends AbstractServer implements Connection.Listener {

  private static final Logger log = LoggerFactory.getLogger(Monitor.class);

  public static void main(String[] args) throws Exception {
    AbstractServer.startServer(new Monitor(new ServerOpts(), args), log);
  }

  Monitor(ServerOpts opts, String[] args) {
    super(ServerId.Type.MONITOR, opts, ServerContext::new, args);
    this.connStats = new ConnectionStatistics();
    this.fetcher = new InformationFetcher(getContext(), connStats::getConnections);
  }

  private final ConnectionStatistics connStats;
  private final InformationFetcher fetcher;

  private EmbeddedWebServer server;
  private int livePort = 0;

  private ServiceLock monitorLock;

  @Override
  public void run() {
    ServerContext context = getContext();
    int[] ports = getConfiguration().getPort(Property.MONITOR_PORT);
    String rootContext = getConfiguration().get(Property.MONITOR_ROOT_CONTEXT);
    // Needs leading slash in order to property create rest endpoint requests
    Preconditions.checkArgument(rootContext.startsWith("/"),
        "Root context: \"%s\" does not have a leading '/'", rootContext);
    for (int port : ports) {
      try {
        log.debug("Trying monitor on port {}", port);
        server = new EmbeddedWebServer(this, port);
        server.addServlet(getResourcesServlet(), "/resources/*");
        server.addServlet(getRestV2Servlet(), "/rest-v2/*");
        server.addServlet(getViewServlet(), "/*");
        server.start();
        livePort = port;
        break;
      } catch (Exception ex) {
        log.error("Unable to start embedded web server", ex);
      }
    }
    if (!server.isRunning()) {
      throw new RuntimeException(
          "Unable to start embedded web server on ports: " + Arrays.toString(ports));
    } else {
      log.debug("Monitor listening on {}:{}", server.getHostName(), livePort);
    }

    HostAndPort advertiseAddress = getAdvertiseAddress();
    if (advertiseAddress == null) {
      // use the bind address from the connector, unless it's null or 0.0.0.0
      String advertiseHost = server.getHostName();
      if (advertiseHost == null || advertiseHost == ServerOpts.BIND_ALL_ADDRESSES) {
        try {
          advertiseHost = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
          throw new RuntimeException("Unable to get hostname for advertise address", e);
        }
      }
      updateAdvertiseAddress(HostAndPort.fromParts(advertiseHost, livePort));
    } else {
      updateAdvertiseAddress(HostAndPort.fromParts(advertiseAddress.getHost(), livePort));
    }
    HostAndPort monitorHostAndPort = getAdvertiseAddress();
    log.debug("Using {} to advertise monitor location in ZooKeeper", monitorHostAndPort);

    try {
      getMonitorLock(monitorHostAndPort);
    } catch (Exception e) {
      log.error("Failed to get Monitor ZooKeeper lock");
      throw new RuntimeException(e);
    }
    getContext().setServiceLock(monitorLock);

    MetricsInfo metricsInfo = getContext().getMetricsInfo();
    metricsInfo.addMetricsProducers(this);
    metricsInfo.init(MetricsInfo.serviceTags(getContext().getInstanceName(), getApplicationName(),
        monitorHostAndPort, getResourceGroup()));

    // Needed to support the existing zk monitor address format
    if (!rootContext.endsWith("/")) {
      rootContext = rootContext + "/";
    }
    try {
      var uri = new URI(server.isSecure() ? "https" : "http", null, monitorHostAndPort.getHost(),
          server.getPort(), rootContext, null, null);
      final ZooReaderWriter zoo = context.getZooSession().asReaderWriter();
      // Delete before we try to re-create in case the previous session hasn't yet expired
      zoo.delete(Constants.ZMONITOR_HTTP_ADDR);
      zoo.putEphemeralData(Constants.ZMONITOR_HTTP_ADDR, uri.toString().getBytes(UTF_8));
      log.info("Set monitor address in zookeeper to {}", uri);
    } catch (Exception ex) {
      log.error("Unable to advertise monitor HTTP address in zookeeper", ex);
    }

    Threads.createCriticalThread("Metric Fetcher Thread", fetcher).start();

    while (!isShutdownRequested()) {
      if (Thread.currentThread().isInterrupted()) {
        log.info("Server process thread has been interrupted, shutting down");
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        log.info("Interrupt Exception received, shutting down");
        gracefulShutdown(context.rpcCreds());
      }
    }

    server.stop();
    log.info("stop requested. exiting ... ");
  }

  private ServletHolder getResourcesServlet() {
    ServletHolder holder = new ServletHolder("resources", ResourceServlet.class);
    holder.setInitParameter("dirAllowed", "false");
    holder.setInitParameter("baseResource",
        Monitor.class.getResource("resources").toExternalForm());
    return holder;
  }

  public static class MonitorFactory extends AbstractBinder implements Factory<Monitor> {

    private final Monitor monitor;

    public MonitorFactory(Monitor monitor) {
      this.monitor = monitor;
    }

    @Override
    public Monitor provide() {
      return monitor;
    }

    @Override
    public void dispose(Monitor instance) {}

    @Override
    protected void configure() {
      bindFactory(this).to(Monitor.class).in(Singleton.class);
    }
  }

  private ServletHolder getViewServlet() {
    final ResourceConfig rc = new ResourceConfig().packages("org.apache.accumulo.monitor.view")
        .register(new MonitorFactory(this))
        .register(new LoggingFeature(java.util.logging.Logger.getLogger(this.getClass().getName())))
        .register(FreemarkerMvcFeature.class)
        .property(MvcFeature.TEMPLATE_BASE_PATH, "/org/apache/accumulo/monitor/templates");
    return new ServletHolder(new ServletContainer(rc));
  }

  private ServletHolder getRestV2Servlet() {
    final ResourceConfig rc = new ResourceConfig().packages("org.apache.accumulo.monitor.next")
        .register(new MonitorFactory(this))
        .register(new LoggingFeature(java.util.logging.Logger.getLogger(this.getClass().getName())))
        .register(JacksonFeature.class);
    return new ServletHolder(new ServletContainer(rc));
  }

  /**
   * Get the monitor lock in ZooKeeper
   */
  private void getMonitorLock(HostAndPort monitorLocation)
      throws KeeperException, InterruptedException {
    ServerContext context = getContext();
    final var monitorLockPath = context.getServerPaths().createMonitorPath();

    // Ensure that everything is kosher with ZK as this has changed.
    ZooReaderWriter zoo = context.getZooSession().asReaderWriter();
    if (zoo.exists(Constants.ZMONITOR)) {
      byte[] data = zoo.getData(Constants.ZMONITOR);
      // If the node isn't empty, it's from a previous install (has hostname:port for HTTP server)
      if (data.length != 0) {
        // Recursively delete from that parent node
        zoo.recursiveDelete(Constants.ZMONITOR, NodeMissingPolicy.SKIP);

        // And then make the nodes that we expect for the incoming ephemeral nodes
        zoo.putPersistentData(Constants.ZMONITOR, new byte[0], NodeExistsPolicy.FAIL);
        zoo.putPersistentData(monitorLockPath.toString(), new byte[0], NodeExistsPolicy.FAIL);
      } else if (!zoo.exists(monitorLockPath.toString())) {
        // monitor node in ZK exists and is empty as we expect
        // but the monitor/lock node does not
        zoo.putPersistentData(monitorLockPath.toString(), new byte[0], NodeExistsPolicy.FAIL);
      }
    } else {
      // 1.5.0 and earlier
      zoo.putPersistentData(Constants.ZMONITOR, new byte[0], NodeExistsPolicy.FAIL);
      if (!zoo.exists(monitorLockPath.toString())) {
        // Somehow the monitor node exists but not monitor/lock
        zoo.putPersistentData(monitorLockPath.toString(), new byte[0], NodeExistsPolicy.FAIL);
      }
    }

    // Get a ZooLock for the monitor
    UUID zooLockUUID = UUID.randomUUID();
    monitorLock = new ServiceLock(context.getZooSession(), monitorLockPath, zooLockUUID);
    HAServiceLockWatcher monitorLockWatcher =
        new HAServiceLockWatcher(Type.MONITOR, () -> isShutdownRequested());

    while (true) {
      monitorLock.lock(monitorLockWatcher,
          new ServiceLockData(zooLockUUID,
              monitorLocation.getHost() + ":" + monitorLocation.getPort(), ThriftService.NONE,
              this.getResourceGroup()));

      monitorLockWatcher.waitForChange();

      if (monitorLockWatcher.isLockAcquired()) {
        break;
      }

      if (!monitorLockWatcher.isFailedToAcquireLock()) {
        throw new IllegalStateException("monitor lock in unknown state");
      }

      monitorLock.tryToCancelAsyncLockOrUnlock();

      sleepUninterruptibly(
          context.getConfiguration().getTimeInMillis(Property.MONITOR_LOCK_CHECK_INTERVAL),
          TimeUnit.MILLISECONDS);
    }

    log.info("Got Monitor lock.");
  }

  @Override
  public ServiceLock getLock() {
    return monitorLock;
  }

  public InformationFetcher getInformationFetcher() {
    return fetcher;
  }

  @Override
  public void onOpened(Connection connection) {
    fetcher.newConnectionEvent();
  }

  @Override
  public void onClosed(Connection connection) {
    // do nothing
  }

  public ConnectionStatistics getConnectionStatisticsBean() {
    return this.connStats;
  }

}
