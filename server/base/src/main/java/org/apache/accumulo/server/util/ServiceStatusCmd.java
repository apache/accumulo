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
import static org.apache.accumulo.core.lock.ServiceLockData.ThriftService.TABLET_SCAN;
import static org.apache.accumulo.core.lock.ServiceLockData.ThriftService.TSERV;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.serviceStatus.ServiceStatusReport;
import org.apache.accumulo.server.util.serviceStatus.StatusSummary;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public void execute(final ServerContext context, final boolean json, final boolean noHosts) {

    ZooReader zooReader = context.getZooSession().asReader();

    LOG.trace("zooRoot: {}", ZooUtil.getRoot(context.getInstanceID()));

    final Map<ServiceStatusReport.ReportKey,StatusSummary> services = new TreeMap<>();

    services.put(ServiceStatusReport.ReportKey.MANAGER,
        getStatusSummary(ServiceStatusReport.ReportKey.MANAGER, zooReader));
    services.put(ServiceStatusReport.ReportKey.MONITOR,
        getStatusSummary(ServiceStatusReport.ReportKey.MONITOR, zooReader));
    services.put(ServiceStatusReport.ReportKey.T_SERVER,
        getServerHostStatus(zooReader, ServiceStatusReport.ReportKey.T_SERVER, TSERV));
    services.put(ServiceStatusReport.ReportKey.S_SERVER,
        getServerHostStatus(zooReader, ServiceStatusReport.ReportKey.S_SERVER, TABLET_SCAN));
    services.put(ServiceStatusReport.ReportKey.COORDINATOR,
        getStatusSummary(ServiceStatusReport.ReportKey.COORDINATOR, zooReader));
    services.put(ServiceStatusReport.ReportKey.COMPACTOR, getCompactorHosts(zooReader));
    services.put(ServiceStatusReport.ReportKey.GC,
        getStatusSummary(ServiceStatusReport.ReportKey.GC, zooReader));

    ServiceStatusReport report = new ServiceStatusReport(services, noHosts);

    if (json) {
      System.out.println(report.toJson());
    } else {
      StringBuilder sb = new StringBuilder(8192);
      report.report(sb);
      System.out.println(sb);
    }
  }

  /**
   * handles paths for tservers and servers with the lock stored beneath the host: port like:
   * {@code /accumulo/IID/[tservers | sservers]/HOST:PORT/[LOCK]}
   */
  @VisibleForTesting
  StatusSummary getServerHostStatus(final ZooReader zooReader,
      ServiceStatusReport.ReportKey displayNames, ServiceLockData.ThriftService serviceType) {
    AtomicInteger errorSum = new AtomicInteger(0);

    // Set<String> hostNames = new TreeSet<>();
    Set<String> groupNames = new TreeSet<>();
    Map<String,Set<String>> hostsByGroups = new TreeMap<>();

    var zkPath = displayNames.getZkPath();
    var nodeNames = readNodeNames(zooReader, zkPath);

    nodeNames.getData().forEach(host -> {
      var lock = readNodeNames(zooReader, zkPath + "/" + host);
      lock.getData().forEach(l -> {
        var nodeData = readNodeData(zooReader, zkPath + "/" + host + "/" + l);
        int err = nodeData.getErrorCount();
        if (err > 0) {
          errorSum.addAndGet(nodeData.getErrorCount());
        } else {

          ServiceLockData.ServiceDescriptors sld =
              ServiceLockData.parseServiceDescriptors(nodeData.getData());

          sld.getServices().forEach(sd -> {
            if (serviceType == sd.getService()) {
              groupNames.add(sd.getGroup());
              hostsByGroups.computeIfAbsent(sd.getGroup(), set -> new TreeSet<>())
                  .add(sd.getAddress());
            }
          });
        }
      });
      errorSum.addAndGet(lock.getFirst());
    });
    return new StatusSummary(displayNames, groupNames, hostsByGroups, errorSum.get());
  }

  /**
   * Used to return status information when path is {@code /accumulo/IID/SERVICE_NAME/lock} like
   * manager, monitor and others
   *
   * @return service status
   */
  @VisibleForTesting
  StatusSummary getStatusSummary(ServiceStatusReport.ReportKey displayNames, ZooReader zooReader) {
    var result = readAllNodesData(zooReader, displayNames.getZkPath());
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
   * Pull host:port from path {@code /accumulo/IID/compactors/[QUEUE][host:port]}
   */
  @VisibleForTesting
  StatusSummary getCompactorHosts(final ZooReader zooReader) {
    final AtomicInteger errors = new AtomicInteger(0);

    Map<String,Set<String>> hostsByGroups = new TreeMap<>();

    // get group names
    Result<Set<String>> queueNodes = readNodeNames(zooReader, Constants.ZCOMPACTORS);
    errors.addAndGet(queueNodes.getErrorCount());
    Set<String> queues = new TreeSet<>(queueNodes.getData());

    queues.forEach(group -> {
      var hostNames = readNodeNames(zooReader, Constants.ZCOMPACTORS + "/" + group);
      errors.addAndGet(hostNames.getErrorCount());
      Collection<String> hosts = hostNames.getData();
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

  /**
   * Provides explicit method names instead of generic getFirst to get the error count and getSecond
   * hosts information
   *
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
