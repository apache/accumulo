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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ServerServices;
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

    services.put(ServiceStatusReport.ReportKey.MANAGER, getManagerStatus(zooReader, zooRoot));
    services.put(ServiceStatusReport.ReportKey.MONITOR, getMonitorStatus(zooReader, zooRoot));
    services.put(ServiceStatusReport.ReportKey.T_SERVER, getTServerStatus(zooReader, zooRoot));
    services.put(ServiceStatusReport.ReportKey.S_SERVER, getScanServerStatus(zooReader, zooRoot));
    services.put(ServiceStatusReport.ReportKey.COORDINATOR,
        getCoordinatorStatus(zooReader, zooRoot));
    services.put(ServiceStatusReport.ReportKey.COMPACTOR, getCompactorStatus(zooReader, zooRoot));
    services.put(ServiceStatusReport.ReportKey.GC, getGcStatus(zooReader, zooRoot));

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
   * lock data providing host:port.
   */
  @VisibleForTesting
  StatusSummary getManagerStatus(final ZooReader zooReader, String zRootPath) {
    String lockPath = zRootPath + Constants.ZMANAGER_LOCK;
    return getStatusSummary(ServiceStatusReport.ReportKey.MANAGER, zooReader, lockPath);
  }

  /**
   * The monitor paths in ZooKeeper are: {@code /accumulo/[IID]/monitor/lock/zlock#[NUM]} with the
   * lock data providing host:port.
   */
  @VisibleForTesting
  StatusSummary getMonitorStatus(final ZooReader zooReader, String zRootPath) {
    String lockPath = zRootPath + Constants.ZMONITOR_LOCK;
    return getStatusSummary(ServiceStatusReport.ReportKey.MONITOR, zooReader, lockPath);
  }

  /**
   * The tserver paths in ZooKeeper are: {@code /accumulo/[IID]/tservers/[host:port]/zlock#[NUM]}
   * with the lock data providing TSERV_CLIENT=host:port.
   */
  @VisibleForTesting
  StatusSummary getTServerStatus(final ZooReader zooReader, String zRootPath) {
    String lockPath = zRootPath + Constants.ZTSERVERS;
    return getServerHostStatus(zooReader, lockPath, ServiceStatusReport.ReportKey.T_SERVER);
  }

  /**
   * The sserver paths in ZooKeeper are: {@code /accumulo/[IID]/sservers/[host:port]/zlock#[NUM]}
   * with the lock data providing [UUID],[GROUP]
   */
  @VisibleForTesting
  StatusSummary getScanServerStatus(final ZooReader zooReader, String zRootPath) {
    String lockPath = zRootPath + Constants.ZSSERVERS;
    return getServerHostStatus(zooReader, lockPath, ServiceStatusReport.ReportKey.S_SERVER);
  }

  /**
   * handles paths for tservers and servers with the lock stored beneath the host: port like:
   * {@code /accumulo/IID/[tservers | sservers]/HOST:PORT/[LOCK]}
   */
  private StatusSummary getServerHostStatus(final ZooReader zooReader, String basePath,
      ServiceStatusReport.ReportKey displayNames) {
    AtomicInteger errorSum = new AtomicInteger(0);

    // Set<String> hostNames = new TreeSet<>();
    Set<String> groupNames = new TreeSet<>();
    Map<String,Set<String>> hostsByGroups = new TreeMap<>();

    var nodeNames = readNodeNames(zooReader, basePath);

    nodeNames.getHosts().forEach(host -> {
      var lock = readNodeNames(zooReader, basePath + "/" + host);
      lock.getHosts().forEach(l -> {
        var nodeData = readNodeData(zooReader, basePath + "/" + host + "/" + l);
        int err = nodeData.getErrorCount();
        if (err > 0) {
          errorSum.addAndGet(nodeData.getErrorCount());
        } else {
          // process resource groups
          String[] tokens = nodeData.getHosts().split(",");
          if (tokens.length == 2) {
            String groupName = tokens[1];
            groupNames.add(groupName);
            hostsByGroups.computeIfAbsent(groupName, s -> new TreeSet<>()).add(host);
          } else {
            hostsByGroups.computeIfAbsent(NO_GROUP_TAG, s -> new TreeSet<>()).add(host);
          }
        }

      });
      errorSum.addAndGet(lock.getFirst());
    });
    return new StatusSummary(displayNames, groupNames, hostsByGroups, errorSum.get());
  }

  /**
   * The gc paths in ZooKeeper are: {@code /accumulo/[IID]/gc/lock/zlock#[NUM]} with the lock data
   * providing GC_CLIENT=host:port
   */
  @VisibleForTesting
  StatusSummary getGcStatus(final ZooReader zooReader, String zRootPath) {
    String lockPath = zRootPath + Constants.ZGC_LOCK;
    var temp = getStatusSummary(ServiceStatusReport.ReportKey.GC, zooReader, lockPath);
    // remove GC_CLIENT= from displayed host:port
    Set<String> hosts =
        new TreeSet<>(stripServiceName(temp.getServiceByGroups().get(NO_GROUP_TAG)));

    Map<String,Set<String>> hostByGroup = new TreeMap<>();
    hostByGroup.put(NO_GROUP_TAG, hosts);

    return new StatusSummary(temp.getServiceType(), temp.getResourceGroups(), hostByGroup,
        temp.getErrorCount());

  }

  /**
   * ServerServices writes lock data as [SERVICE]=host. This strips the [SERVICE]= from the string.
   *
   * @return a sort set of host names.
   */
  private Set<String> stripServiceName(Set<String> hostnames) {
    return hostnames.stream().map(h -> {
      if (h.contains(ServerServices.SEPARATOR_CHAR)) {
        return h.substring(h.indexOf(ServerServices.SEPARATOR_CHAR) + 1);
      }
      return h;
    }).collect(Collectors.toCollection(TreeSet::new));
  }

  /**
   * The coordinator paths in ZooKeeper are: {@code /accumulo/[IID]/coordinators/lock/zlock#[NUM]}
   * with the lock data providing host:port
   */
  @VisibleForTesting
  StatusSummary getCoordinatorStatus(final ZooReader zooReader, String zRootPath) {
    String lockPath = zRootPath + Constants.ZCOORDINATOR_LOCK;
    return getStatusSummary(ServiceStatusReport.ReportKey.COORDINATOR, zooReader, lockPath);
  }

  /**
   * The compactor paths in ZooKeeper are:
   * {@code /accumulo/[IID]/compactors/[QUEUE_NAME]/host:port/zlock#[NUM]} with the host:port pulled
   * from the path
   */
  @VisibleForTesting
  StatusSummary getCompactorStatus(final ZooReader zooReader, String zRootPath) {
    String lockPath = zRootPath + Constants.ZCOMPACTORS;
    return getCompactorHosts(zooReader, lockPath);
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
    byGroup.put(NO_GROUP_TAG, result.getHosts());
    return new StatusSummary(displayNames, Set.of(), byGroup, result.getErrorCount());
  }

  /**
   * Pull host:port from path {@code /accumulo/IID/compactors/[QUEUE][host:port]}
   */
  private StatusSummary getCompactorHosts(final ZooReader zooReader, final String zRootPath) {
    final AtomicInteger errors = new AtomicInteger(0);

    Map<String,Set<String>> hostsByGroups = new TreeMap<>();

    // get group names
    Result<Set<String>> queueNodes = readNodeNames(zooReader, zRootPath);
    errors.addAndGet(queueNodes.getErrorCount());
    Set<String> queues = new TreeSet<>(queueNodes.getHosts());

    queues.forEach(group -> {
      var hostNames = readNodeNames(zooReader, zRootPath + "/" + group);
      errors.addAndGet(hostNames.getErrorCount());
      Collection<String> hosts = hostNames.getHosts();
      hosts.forEach(host -> {
        hostsByGroups.computeIfAbsent(group, set -> new TreeSet<>()).add(host);
      });
    });

    return new StatusSummary(ServiceStatusReport.ReportKey.COMPACTOR, queues, hostsByGroups,
        errors.get());
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
    Set<String> hosts = new TreeSet<>();
    final AtomicInteger errorCount = new AtomicInteger(0);
    try {
      var locks = zooReader.getChildren(path);
      locks.forEach(lock -> {
        var nodeData = readNodeData(zooReader, path + "/" + lock);
        int err = nodeData.getErrorCount();
        if (err > 0) {
          errorCount.addAndGet(nodeData.getErrorCount());
        } else {
          hosts.add(nodeData.getHosts());
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
    return new Result<>(errorCount.get(), hosts);
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

    public B getHosts() {
      return getSecond();
    }
  }
}
