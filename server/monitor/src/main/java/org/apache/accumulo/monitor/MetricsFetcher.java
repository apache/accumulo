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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.core.metrics.thrift.MetricService.Client;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.ThreadPools.ExecutionError;
import org.apache.accumulo.server.ServerContext;
import org.apache.thrift.TException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.net.HostAndPort;

public class MetricsFetcher implements RemovalListener<ServerId,MetricResponse>, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsFetcher.class);

  private static class GcServerId extends ServerId {
    private GcServerId(String resourceGroup, String host, int port) {
      // TODO: This is a little wonky, Type.GC does not exist in the public API,
      super(ServerId.Type.MANAGER, resourceGroup, host, port);
    }
  }

  private class MetricFetcher implements Runnable {

    private final ServerContext ctx;
    private final ServerId server;

    private MetricFetcher(ServerContext ctx, ServerId server) {
      this.ctx = ctx;
      this.server = server;
    }

    @Override
    public void run() {
      try {
        Client metricsClient = ThriftUtil.getClient(ThriftClientTypes.METRICS,
            HostAndPort.fromParts(server.getHost(), server.getPort()), ctx);
        try {
          MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(), ctx.rpcCreds());
          problemHosts.remove(server);
          allMetrics.put(server, response);
          switch (response.serverType) {
            case COMPACTOR:
              compactors.computeIfAbsent(response.getResourceGroup(), (rg) -> new HashSet<>())
                  .add(server);
              break;
            case GARBAGE_COLLECTOR:
              if (gc.get() == null || !gc.get().equals(server)) {
                gc.set(server);
              }
              break;
            case MANAGER:
              if (manager.get() == null || !manager.get().equals(server)) {
                manager.set(server);
              }
              break;
            case SCAN_SERVER:
              sservers.computeIfAbsent(response.getResourceGroup(), (rg) -> new HashSet<>())
                  .add(server);
              break;
            case TABLET_SERVER:
              tservers.computeIfAbsent(response.getResourceGroup(), (rg) -> new HashSet<>())
                  .add(server);
              break;
            default:
              break;

          }
        } finally {
          ThriftUtil.returnClient(metricsClient, ctx);
        }
      } catch (TException e) {
        LOG.warn("Error trying to get metrics from server: {}", server);
        // Error talking to server, add to problem hosts and remove from everything else
        problemHosts.add(server);
        gc.compareAndSet(server, null);
        manager.compareAndSet(server, null);
        compactors.getOrDefault(server.getResourceGroup(), EMPTY_MUTABLE_SET).remove(server);
        sservers.getOrDefault(server.getResourceGroup(), EMPTY_MUTABLE_SET).remove(server);
        tservers.getOrDefault(server.getResourceGroup(), EMPTY_MUTABLE_SET).remove(server);
      }
    }

  }

  private static final Set<ServerId> EMPTY_MUTABLE_SET = new HashSet<>();

  private final String poolName = "MonitorMetricsThreadPool";
  private final ThreadPoolExecutor pool = ThreadPools.getServerThreadPools()
      .getPoolBuilder(poolName).numCoreThreads(10).withTimeOut(30, SECONDS).build();

  private final ServerContext ctx;
  private final Cache<ServerId,MetricResponse> allMetrics;
  private final Set<ServerId> problemHosts = new HashSet<>();
  private final AtomicReference<ServerId> manager = new AtomicReference<>();
  private final AtomicReference<ServerId> gc = new AtomicReference<>();
  private final ConcurrentHashMap<String,Set<ServerId>> compactors = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String,Set<ServerId>> sservers = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String,Set<ServerId>> tservers = new ConcurrentHashMap<>();

  public MetricsFetcher(ServerContext ctx) {
    this.ctx = ctx;
    this.allMetrics = Caffeine.newBuilder().executor(pool).scheduler(Scheduler.systemScheduler())
        .expireAfterWrite(Duration.ofMinutes(10)).removalListener(this::onRemoval).build();
  }

  @Override
  public void onRemoval(@Nullable ServerId server, @Nullable MetricResponse response,
      RemovalCause cause) {
    if (server == null) {
      return;
    }
    if (server instanceof GcServerId) {
      gc.compareAndSet(server, null);
    } else {
      switch (server.getType()) {
        case COMPACTOR:
          compactors.getOrDefault(server.getResourceGroup(), EMPTY_MUTABLE_SET).remove(server);
          break;
        case MANAGER:
          manager.compareAndSet(server, null);
          break;
        case SCAN_SERVER:
          sservers.getOrDefault(server.getResourceGroup(), EMPTY_MUTABLE_SET).remove(server);
          break;
        case TABLET_SERVER:
          tservers.getOrDefault(server.getResourceGroup(), EMPTY_MUTABLE_SET).remove(server);
          break;
        default:
          LOG.error("Unhandled server type sent to onRemoval: {}", server);
          break;
      }
    }
  }

  @Override
  public void run() {

    while (true) {

      final List<Future<?>> futures = new ArrayList<>();

      for (ServerId.Type type : ServerId.Type.values()) {
        for (ServerId server : this.ctx.instanceOperations().getServers(type)) {
          futures.add(this.pool.submit(new MetricFetcher(this.ctx, server)));
        }
      }
      ThreadPools.resizePool(pool, () -> Math.max(20, (futures.size() / 20)), poolName);

      // GC is not a public type, add it
      ServiceLockPath zgcPath = this.ctx.getServerPaths().getGarbageCollector(true);
      if (zgcPath != null) {
        Optional<ServiceLockData> sld = this.ctx.getZooCache().getLockData(zgcPath);
        if (sld.isPresent()) {
          String location = sld.orElseThrow().getAddressString(ThriftService.GC);
          String resourceGroup = sld.orElseThrow().getGroup(ThriftService.GC);
          HostAndPort hp = HostAndPort.fromString(location);
          futures.add(this.pool.submit(new MetricFetcher(this.ctx,
              new GcServerId(resourceGroup, hp.getHost(), hp.getPort()))));
        }
      }

      while (futures.size() > 0) {
        Iterator<Future<?>> iter = futures.iterator();
        while (iter.hasNext()) {
          Future<?> future = iter.next();
          if (future.isDone()) {
            iter.remove();
            try {
              future.get();
            } catch (InterruptedException | ExecutionException e) {
              // throwing an error so the Monitor will die
              throw new ExecutionError("Error getting metrics from server", e);
            }
          }
        }
        if (futures.size() > 0) {
          UtilWaitThread.sleep(3_000);
        }
      }

      UtilWaitThread.sleep(30_000);
    }

  }

  public Set<String> getResourceGroups() {
    Set<String> groups = new HashSet<>();
    groups.add(Constants.DEFAULT_RESOURCE_GROUP_NAME);
    groups.addAll(compactors.keySet());
    groups.addAll(sservers.keySet());
    groups.addAll(tservers.keySet());
    return groups;
  }

  public Collection<ServerId> getProblemHosts() {
    return problemHosts;
  }

  public Collection<MetricResponse> getAll() {
    return allMetrics.asMap().values();
  }

  public MetricResponse getManager() {
    ServerId s = manager.get();
    if (s == null) {
      return null;
    }
    return allMetrics.asMap().get(s);
  }

  public MetricResponse getGarbageCollector() {
    ServerId s = gc.get();
    if (s == null) {
      return null;
    }
    return allMetrics.asMap().get(s);
  }

  public Collection<MetricResponse> getCompactors(String resourceGroup) {
    if (!compactors.containsKey(resourceGroup)) {
      throw new IllegalArgumentException("Resource Group " + resourceGroup + " not found.");
    }
    return allMetrics.getAllPresent(compactors.get(resourceGroup)).values();
  }

  public Collection<MetricResponse> getSServers(String resourceGroup) {
    if (!sservers.containsKey(resourceGroup)) {
      throw new IllegalArgumentException("Resource Group " + resourceGroup + " not found.");
    }
    return allMetrics.getAllPresent(sservers.get(resourceGroup)).values();
  }

  public Collection<MetricResponse> getTServers(String resourceGroup) {
    if (!tservers.containsKey(resourceGroup)) {
      throw new IllegalArgumentException("Resource Group " + resourceGroup + " not found.");
    }
    return allMetrics.getAllPresent(tservers.get(resourceGroup)).values();
  }

}
