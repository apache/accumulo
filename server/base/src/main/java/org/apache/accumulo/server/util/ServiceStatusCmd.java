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
package org.apache.accumulo.server.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.serviceStatus.ServiceStatusReport;
import org.apache.accumulo.server.util.serviceStatus.StatusSummary;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;

public class ServiceStatusCmd {

  // used when grouping by resource group when there is no group.
  public static final String NO_GROUP_TAG = "NO_GROUP";

  private static final Logger LOG = LoggerFactory.getLogger(ServiceStatusCmd.class);

  public ServiceStatusCmd() {}

  /**
   * Read the service statuses from ZooKeeper, build the status report and then output the report to
   * stdout.
   */
  public void execute(final ServerContext context, final Opts opts) {

    ZooReader zooReader = context.getZooReader();

    final String zooRoot = context.getZooKeeperRoot();
    LOG.trace("zooRoot: {}", zooRoot);

    final Map<ServiceStatusReport.ReportKey,StatusSummary> services = new TreeMap<>();

    services.put(ServiceStatusReport.ReportKey.MANAGER, getManagerStatus(zooReader, context));
    services.put(ServiceStatusReport.ReportKey.MONITOR, getMonitorStatus(zooReader, context));
    services.put(ServiceStatusReport.ReportKey.T_SERVER, getTServerStatus(context));
    services.put(ServiceStatusReport.ReportKey.S_SERVER, getScanServerStatus(context));
    services.put(ServiceStatusReport.ReportKey.COMPACTOR, getCompactorStatus(context));
    services.put(ServiceStatusReport.ReportKey.GC, getGcStatus(zooReader, context));

    ServiceStatusReport report = new ServiceStatusReport(services, opts.noHosts);

    if (opts.json) {
      System.out.println(report.toJson());
    } else {
      StringBuilder sb = new StringBuilder(8192);
      report.report(sb);
      System.out.println(sb);
    }
  }

  /**
   * The manager paths in ZooKeeper are: {@code /accumulo/[IID]/managers/lock/zlock#[NUM]} with the
   * lock data providing a service descriptor with host and port.
   */
  @VisibleForTesting
  StatusSummary getManagerStatus(ZooReader zooReader, ServerContext context) {
    String lockPath = context.getServerPaths().createManagerPath().toString();
    return getStatusSummary(ServiceStatusReport.ReportKey.MANAGER, zooReader, lockPath);
  }

  /**
   * The monitor paths in ZooKeeper are: {@code /accumulo/[IID]/monitor/lock/zlock#[NUM]} with the
   * lock data providing a service descriptor with host and port.
   */
  @VisibleForTesting
  StatusSummary getMonitorStatus(final ZooReader zooReader, ServerContext context) {
    String lockPath = context.getServerPaths().createMonitorPath().toString();
    return getStatusSummary(ServiceStatusReport.ReportKey.MONITOR, zooReader, lockPath);
  }

  /**
   * The tserver paths in ZooKeeper are:
   * {@code /accumulo/[IID]/tservers/[resourceGroup]/[host:port]/zlock#[NUM]} with the lock data
   * providing TSERV_CLIENT=host:port.
   */
  @VisibleForTesting
  StatusSummary getTServerStatus(ServerContext context) {
    final AtomicInteger errors = new AtomicInteger(0);
    final Map<String,Set<String>> hostsByGroups = new TreeMap<>();
    final Set<ServiceLockPath> compactors =
        context.getServerPaths().getTabletServer(Optional.empty(), Optional.empty(), true);
    compactors.forEach(c -> hostsByGroups
        .computeIfAbsent(c.getResourceGroup(), (k) -> new TreeSet<>()).add(c.getServer()));
    return new StatusSummary(ServiceStatusReport.ReportKey.T_SERVER, hostsByGroups.keySet(),
        hostsByGroups, errors.get());
  }

  /**
   * The sserver paths in ZooKeeper are:
   * {@code /accumulo/[IID]/sservers/[resourceGroup]/[host:port]/zlock#[NUM]} with the lock data
   * providing [UUID],[GROUP]
   */
  @VisibleForTesting
  StatusSummary getScanServerStatus(ServerContext context) {
    final AtomicInteger errors = new AtomicInteger(0);
    final Map<String,Set<String>> hostsByGroups = new TreeMap<>();
    final Set<ServiceLockPath> scanServers =
        context.getServerPaths().getScanServer(Optional.empty(), Optional.empty(), true);
    scanServers.forEach(c -> hostsByGroups
        .computeIfAbsent(c.getResourceGroup(), (k) -> new TreeSet<>()).add(c.getServer()));
    return new StatusSummary(ServiceStatusReport.ReportKey.S_SERVER, hostsByGroups.keySet(),
        hostsByGroups, errors.get());
  }

  /**
   * The gc paths in ZooKeeper are: {@code /accumulo/[IID]/gc/lock/zlock#[NUM]} with the lock data
   * providing GC_CLIENT=host:port
   */
  @VisibleForTesting
  StatusSummary getGcStatus(final ZooReader zooReader, ServerContext context) {
    String lockPath = context.getServerPaths().createGarbageCollectorPath().toString();
    return getStatusSummary(ServiceStatusReport.ReportKey.GC, zooReader, lockPath);
  }

  /**
   * The compactor paths in ZooKeeper are:
   * {@code /accumulo/[IID]/compactors/[resourceGroup]/host:port/zlock#[NUM]} with the host:port
   * pulled from the path
   */
  @VisibleForTesting
  StatusSummary getCompactorStatus(ServerContext context) {
    final AtomicInteger errors = new AtomicInteger(0);
    final Map<String,Set<String>> hostsByGroups = new TreeMap<>();
    final Set<ServiceLockPath> compactors =
        context.getServerPaths().getCompactor(Optional.empty(), Optional.empty(), true);
    compactors.forEach(c -> hostsByGroups
        .computeIfAbsent(c.getResourceGroup(), (k) -> new TreeSet<>()).add(c.getServer()));
    return new StatusSummary(ServiceStatusReport.ReportKey.COMPACTOR, hostsByGroups.keySet(),
        hostsByGroups, errors.get());
  }

  /**
   * Used to return status information when path is {@code /accumulo/IID/SERVICE_NAME/lock} like
   * manager, monitor and others
   *
   * @return service status
   */
  private StatusSummary getStatusSummary(ServiceStatusReport.ReportKey displayNames,
      ZooReader zooReader, String lockPath) {
    var result = readAllNodesData(zooReader, lockPath);
    Map<String,Set<String>> byGroup = new TreeMap<>();
    result.getData().forEach(data -> {
      ServiceLockData.ServiceDescriptors sld = ServiceLockData.parseServiceDescriptors(data);
      var services = sld.getServices();
      services.forEach(sd -> {
        byGroup.computeIfAbsent(sd.getGroup(), set -> new TreeSet<>()).add(sd.getAddress());
      });
    });
    return new StatusSummary(displayNames, byGroup.keySet(), byGroup, result.getErrorCount());
  }

  /**
   * Read the node names from ZooKeeper. Exceptions are counted but ignored.
   *
   * @return Result with error count, Set of the node names.
   */
  @VisibleForTesting
  Result<Set<String>> readNodeNames(final ZooReader zooReader, final String path) {
    Set<String> nodeNames = new TreeSet<>();
    final AtomicInteger errorCount = new AtomicInteger(0);
    try {
      var children = zooReader.getChildren(path);
      if (children != null) {
        nodeNames.addAll(children);
      }
    } catch (KeeperException | InterruptedException ex) {
      if (Thread.currentThread().isInterrupted()) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(ex);
      }
      errorCount.incrementAndGet();
    }
    return new Result<>(errorCount.get(), nodeNames);
  }

  /**
   * Read the data from a ZooKeeper node, tracking if an error occurred. ZooKeeper's exceptions are
   * counted but otherwise ignored.
   *
   * @return Pair with error count, the node data as String.
   */
  @VisibleForTesting
  Result<String> readNodeData(final ZooReader zooReader, final String path) {
    try {
      byte[] data = zooReader.getData(path);
      return new Result<>(0, new String(data, UTF_8));
    } catch (KeeperException | InterruptedException ex) {
      if (Thread.currentThread().isInterrupted()) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(ex);
      }
      LOG.info("Could not read locks from ZooKeeper for path {}", path, ex);
      return new Result<>(1, "");
    }
  }

  /**
   * Read the data from all ZooKeeper nodes under a ptah, tracking if errors occurred. ZooKeeper's
   * exceptions are counted but otherwise ignored.
   *
   * @return Pair with error count, the data from each node as a String.
   */
  @VisibleForTesting
  Result<Set<String>> readAllNodesData(final ZooReader zooReader, final String path) {
    Set<String> data = new TreeSet<>();
    final AtomicInteger errorCount = new AtomicInteger(0);
    try {
      var locks = zooReader.getChildren(path);
      locks.forEach(lock -> {
        var nodeData = readNodeData(zooReader, path + "/" + lock);
        int err = nodeData.getErrorCount();
        if (err > 0) {
          errorCount.addAndGet(nodeData.getErrorCount());
        } else {
          data.add(nodeData.getData());
        }
      });
    } catch (KeeperException | InterruptedException ex) {
      if (Thread.currentThread().isInterrupted()) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(ex);
      }
      LOG.info("Could not read node names from ZooKeeper for path {}", path, ex);
      errorCount.incrementAndGet();
    }
    return new Result<>(errorCount.get(), data);
  }

  @Parameters(commandDescription = "show service status")
  public static class Opts {
    @Parameter(names = "--json", description = "provide output in json format (--noHosts ignored)")
    boolean json = false;
    @Parameter(names = "--noHosts",
        description = "provide a summary of service counts without host details")
    boolean noHosts = false;
  }

  /**
   * Provides explicit method names instead of generic getFirst to get the error count and getSecond
   * hosts information
   *
   * @param <A> errorCount
   * @param <B> hosts
   */
  private static class Result<B> extends Pair<Integer,B> {
    public Result(Integer errorCount, B hosts) {
      super(errorCount, hosts);
    }

    public Integer getErrorCount() {
      return getFirst();
    }

    public B getData() {
      return getSecond();
    }
  }
}
