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
package org.apache.accumulo.server.util.adminCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.process.thrift.ServerProcessService;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.server.util.ZooZap;
import org.apache.accumulo.server.util.adminCommand.StopServers.StopServersOpts;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.accumulo.start.spi.UsageGroup;
import org.apache.accumulo.start.spi.UsageGroups;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;
import com.google.common.net.HostAndPort;

@AutoService(KeywordExecutable.class)
public class StopServers extends ServerKeywordExecutable<StopServersOpts> {

  private static final Logger LOG = LoggerFactory.getLogger(StopServers.class);

  // This only exists because it is called from ITs
  public static void main(String[] args) throws Exception {
    new StopServers().execute(args);
  }

  static class StopServersOpts extends ServerUtilOpts {

    @Parameter(names = {"-f", "--force"},
        description = "force the given server to stop immediately by removing its lock.  Does not wait for any task the server is currently working.")
    boolean force = false;

    @Parameter(description = "<host[:port]> {<host[:port]> ... }")
    List<String> args = new ArrayList<>();
  }

  public StopServers() {
    super(new StopServersOpts());
  }

  @Override
  public String keyword() {
    return "stop-servers";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroups.ADMIN;
  }

  @Override
  public String description() {
    return "Stop the servers at the given addresses allowing them to complete current task but not start new task.  Hostnames only are no longer supported; you must use <host:port>. To Stop all services on a host, use 'accumulo admin service-status' to list all hosts and then pass them to this command.";
  }

  @Override
  public void execute(JCommander cl, StopServersOpts options) throws Exception {

    ServerContext context = options.getServerContext();
    List<String> hostOnly = new ArrayList<>();
    Set<String> hostAndPort = new TreeSet<>();

    for (var server : options.args) {
      if (server.contains(":")) {
        hostAndPort.add(server);
      } else {
        hostOnly.add(server);
      }
    }

    if (!hostOnly.isEmpty()) {
      // The old impl of this command with the old behavior
      LOG.warn("Stopping by hostname is no longer supported\n\n"
          + "please use <host:port> instead.\n"
          + "To stop all services on host, run 'accumulo admin serviceStatus' to list all host:port values and filter for that host and pass those to 'accumulo admin stop'");
      stopTabletServer(context, hostOnly, options.force);
    }

    if (!hostAndPort.isEmpty()) {
      // New behavior for this command when ports are present, supports more than just tservers. Is
      // also async.
      if (options.force) {
        var zoo = context.getZooSession().asReaderWriter();

        AddressSelector addresses = AddressSelector.matching(hostAndPort::contains);
        List<ServiceLockPath> pathsToRemove = new ArrayList<>();
        pathsToRemove.addAll(
            context.getServerPaths().getCompactor(ResourceGroupPredicate.ANY, addresses, false));
        pathsToRemove.addAll(
            context.getServerPaths().getScanServer(ResourceGroupPredicate.ANY, addresses, false));
        pathsToRemove.addAll(
            context.getServerPaths().getTabletServer(ResourceGroupPredicate.ANY, addresses, false));
        ZooZap.filterSingleton(context, context.getServerPaths().getManager(false), addresses)
            .ifPresent(pathsToRemove::add);
        ZooZap.filterSingleton(context, context.getServerPaths().getGarbageCollector(false),
            addresses).ifPresent(pathsToRemove::add);
        ZooZap.filterSingleton(context, context.getServerPaths().getMonitor(false), addresses)
            .ifPresent(pathsToRemove::add);

        for (var path : pathsToRemove) {
          List<String> children = zoo.getChildren(path.toString());
          for (String child : children) {
            LOG.trace("removing lock {}", path + "/" + child);
            zoo.recursiveDelete(path + "/" + child, ZooUtil.NodeMissingPolicy.SKIP);
          }
        }
      } else {
        for (var server : hostAndPort) {
          signalGracefulShutdown(context, HostAndPort.fromString(server));
        }
      }
    }
  }

  // Visible for tests
  public static void signalGracefulShutdown(final ClientContext context, HostAndPort hp) {
    Objects.requireNonNull(hp, "address not set");
    ServerProcessService.Client client = null;
    try {
      client = ThriftClientTypes.SERVER_PROCESS.getServerProcessConnection(context, LOG,
          hp.getHost(), hp.getPort());
      if (client == null) {
        LOG.warn("Failed to initiate shutdown for {}", hp);
        return;
      }
      client.gracefulShutdown(context.rpcCreds());
      LOG.info("Initiated shutdown for {}", hp);
    } catch (TException e) {
      LOG.warn("Failed to initiate shutdown for {}", hp, e);
    } finally {
      if (client != null) {
        ThriftUtil.returnClient(client, context);
      }
    }
  }

  /**
   * Stops tablet servers by hostname
   *
   * @param context The server context
   * @param servers LIst of hostnames (without ports)
   * @param force Whether to force stop
   * @deprecated Use servers with host:port format instead. To stop all services on a host use
   *             service status command to liat all services, then stop them with host:port format.
   */
  @Deprecated(since = "4.0.0")
  private void stopTabletServer(final ClientContext context, List<String> servers,
      final boolean force) throws AccumuloException, AccumuloSecurityException {
    if (context.instanceOperations().getServers(ServerId.Type.MANAGER).isEmpty()) {
      LOG.info("No managers running. Not attempting safe unload of tserver.");
      return;
    }
    if (servers.isEmpty()) {
      LOG.error("No tablet servers provided.");
      return;
    }

    final ZooCache zc = context.getZooCache();
    Set<ServerId> runningServers;

    for (String server : servers) {
      runningServers = context.instanceOperations().getServers(ServerId.Type.TABLET_SERVER);
      if (runningServers.size() == 1 && !force) {
        LOG.info("Only 1 tablet server running. Not attempting shutdown of {}", server);
        return;
      }
      for (int port : context.getConfiguration().getPort(Property.TSERV_CLIENTPORT)) {
        HostAndPort address = AddressUtil.parseAddress(server, port);
        final String finalServer = qualifyWithZooKeeperSessionId(context, zc, address.toString());
        LOG.info("Stopping server {}", finalServer);
        ThriftClientTypes.MANAGER.executeVoid(context, client -> client
            .shutdownTabletServer(TraceUtil.traceInfo(), context.rpcCreds(), finalServer, force));
      }
    }
  }

  /**
   * Look up the TabletServers in ZooKeeper and try to find a sessionID for this server reference
   *
   * @param hostAndPort The host and port for a TabletServer
   * @return The host and port with the session ID in square-brackets appended, or the original
   *         value.
   */
  static String qualifyWithZooKeeperSessionId(ClientContext context, ZooCache zooCache,
      String hostAndPort) {
    var hpObj = HostAndPort.fromString(hostAndPort);
    Set<ServiceLockPath> paths = context.getServerPaths()
        .getTabletServer(ResourceGroupPredicate.ANY, AddressSelector.exact(hpObj), true);
    if (paths.size() != 1) {
      return hostAndPort;
    }
    long sessionId = ServiceLock.getSessionId(zooCache, paths.iterator().next());
    if (sessionId == 0) {
      return hostAndPort;
    }
    return hostAndPort + "[" + Long.toHexString(sessionId) + "]";
  }

}
