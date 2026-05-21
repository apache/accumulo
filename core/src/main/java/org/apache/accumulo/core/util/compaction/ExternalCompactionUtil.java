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
package org.apache.accumulo.core.util.compaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.COMPACTOR_RUNNING_COMPACTIONS_POOL;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.COMPACTOR_RUNNING_COMPACTION_IDS_POOL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.CompactorService;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.rpc.RpcFuture;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.zookeeper.ZcStat;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class ExternalCompactionUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalCompactionUtil.class);

  /**
   *
   * @return Optional HostAndPort of Coordinator node if found
   */
  public static Optional<HostAndPort> findCompactionCoordinator(ClientContext context) {
    final ServiceLockPath slp = context.getServerPaths().createManagerPath();
    if (slp == null) {
      return Optional.empty();
    }
    return ServiceLock.getLockData(context.getZooCache(), slp, new ZcStat())
        .map(sld -> sld.getAddress(ThriftService.COORDINATOR));
  }

  /**
   * @param context client context
   * @return CompactionCoordinator Thrift client, user has the responsibility to call
   *         {@code ThriftUtil.returnClient(coordinatorClient, context);}
   */
  public static CompactionCoordinatorService.Client getCoordinatorClient(ClientContext context) {
    var coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(context);
    if (coordinatorHost.isEmpty()) {
      throw new IllegalStateException("Unable to find coordinator. Check that it is running.");
    }
    HostAndPort address = coordinatorHost.orElseThrow();
    CompactionCoordinatorService.Client coordinatorClient;
    try {
      coordinatorClient = ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, address, context);
    } catch (TTransportException e) {
      throw new IllegalStateException("Unable to get Compaction coordinator at " + address, e);
    }
    return coordinatorClient;
  }

  /**
   * @return map of group names to compactor addresses
   */
  public static Map<String,Set<HostAndPort>> getCompactorAddrs(ClientContext context) {
    final Map<String,Set<HostAndPort>> groupsAndAddresses = new HashMap<>();
    context.getServerPaths().getCompactor(ResourceGroupPredicate.ANY, AddressSelector.all(), true)
        .forEach(slp -> {
          groupsAndAddresses
              .computeIfAbsent(slp.getResourceGroup().canonical(), (k) -> new HashSet<>())
              .add(HostAndPort.fromString(slp.getServer()));
        });
    return groupsAndAddresses;
  }

  /**
   *
   * @param compactor compactor address
   * @param context client context
   * @return list of active compaction
   * @throws ThriftSecurityException tserver permission error
   */
  public static List<ActiveCompaction> getActiveCompaction(HostAndPort compactor,
      ClientContext context) throws ThriftSecurityException {
    CompactorService.Client client = null;
    try {
      client = ThriftUtil.getClient(ThriftClientTypes.COMPACTOR, compactor, context);
      return client.getActiveCompactions(TraceUtil.traceInfo(), context.rpcCreds());
    } catch (ThriftSecurityException e) {
      throw e;
    } catch (TException e) {
      LOG.debug("Failed to contact compactor {}", compactor, e);
    } finally {
      ThriftUtil.returnClient(client, context);
    }
    return List.of();
  }

  /**
   * Get the compaction currently running on the Compactor
   *
   * @param compactorAddr compactor address
   * @param context context
   * @return external compaction job or null if none running
   */
  public static TExternalCompaction getRunningCompaction(HostAndPort compactorAddr,
      ClientContext context) throws TException {

    CompactorService.Client client = null;
    try {
      client = ThriftUtil.getClient(ThriftClientTypes.COMPACTOR, compactorAddr, context);
      TExternalCompaction current =
          client.getRunningCompaction(TraceUtil.traceInfo(), context.rpcCreds());
      if (current.getJob() != null && current.getJob().getExternalCompactionId() != null) {
        LOG.debug("Compactor {} is running {}", compactorAddr,
            current.getJob().getExternalCompactionId());
        return current;
      }
    } catch (TException e) {
      LOG.debug("Failed to contact compactor {}", compactorAddr, e);
      throw e;
    } finally {
      ThriftUtil.returnClient(client, context);
    }
    return null;
  }

  private static ExternalCompactionId getRunningCompactionId(HostAndPort compactorAddr,
      ClientContext context) {
    CompactorService.Client client = null;
    try {
      client = ThriftUtil.getClient(ThriftClientTypes.COMPACTOR, compactorAddr, context);
      String secid = client.getRunningCompactionId(TraceUtil.traceInfo(), context.rpcCreds());
      if (!secid.isEmpty()) {
        return ExternalCompactionId.of(secid);
      }
    } catch (TException e) {
      LOG.debug("Failed to contact compactor {}", compactorAddr, e);
    } finally {
      ThriftUtil.returnClient(client, context);
    }
    return null;
  }

  /**
   * This method returns information from the Compactors about the job that is currently running.
   * This method will use a thread pool with 16 threads to query the Compactors.
   *
   * @param context server context
   * @param consumer object that will accept TExternalCompaction objects
   */
  public static void getCompactionsRunningOnCompactors(ClientContext context,
      Consumer<TExternalCompaction> consumer) throws InterruptedException {
    final ExecutorService executor = ThreadPools.getServerThreadPools()
        .getPoolBuilder(COMPACTOR_RUNNING_COMPACTIONS_POOL).numCoreThreads(16).build();
    try {
      getCompactionsRunningOnCompactors(context, executor, consumer);
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * This method returns information from the Compactors about the job that is currently running.
   *
   * @param context server context
   * @param executor thread pool executor to use for querying Compactors
   * @param consumer object that will accept TExternalCompaction objects
   * @return list of compactor addresses where RPC failed
   */
  public static List<ServerId> getCompactionsRunningOnCompactors(ClientContext context,
      ExecutorService executor, Consumer<TExternalCompaction> consumer)
      throws InterruptedException {

    final List<RpcFuture<TExternalCompaction>> rcFutures = new ArrayList<>();
    final List<ServerId> failures = new ArrayList<>();

    try {
      Set<ServerId> compactors = context.instanceOperations().getServers(ServerId.Type.COMPACTOR);
      compactors.forEach(s -> {
        final HostAndPort address = HostAndPort.fromParts(s.getHost(), s.getPort());
        Future<TExternalCompaction> future =
            executor.submit(() -> getRunningCompaction(address, context));
        rcFutures.add(new RpcFuture<TExternalCompaction>(future, s));
      });

      while (!rcFutures.isEmpty()) {
        var futureIter = rcFutures.iterator();
        while (futureIter.hasNext()) {
          var future = futureIter.next();
          if (future.isDone()) {
            try {
              TExternalCompaction tec = future.get();
              if (tec != null && tec.getJob() != null
                  && tec.getJob().getExternalCompactionId() != null) {
                consumer.accept(tec);
              }
            } catch (ExecutionException e) {
              LOG.error("Error getting compaction from compactor: " + future.getServer(), e);
              failures.add(future.getServer());
            } finally {
              futureIter.remove();
            }
          }
        }
        Thread.sleep(100);
      }
      return failures;
    } catch (InterruptedException e) {
      // If this thread is interrupted, cancel all remaining tasks
      var futureIter = rcFutures.iterator();
      while (futureIter.hasNext()) {
        var future = futureIter.next();
        future.cancel(true);
      }
      rcFutures.clear();
      throw e;
    }
  }

  public static Set<ExternalCompactionId>
      getCompactionIdsRunningOnCompactors(ClientContext context) {
    final ExecutorService executor = ThreadPools.getServerThreadPools()
        .getPoolBuilder(COMPACTOR_RUNNING_COMPACTION_IDS_POOL).numCoreThreads(16).build();
    List<Future<ExternalCompactionId>> futures = new ArrayList<>();

    context.getServerPaths().getCompactor(ResourceGroupPredicate.ANY, AddressSelector.all(), true)
        .forEach(slp -> {
          final HostAndPort hp = HostAndPort.fromString(slp.getServer());
          futures.add(executor.submit(() -> getRunningCompactionId(hp, context)));
        });
    executor.shutdown();

    HashSet<ExternalCompactionId> runningIds = new HashSet<>();

    futures.forEach(future -> {
      try {
        ExternalCompactionId ceid = future.get();
        if (ceid != null) {
          runningIds.add(ceid);
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new IllegalStateException(e);
      }
    });

    return runningIds;
  }

  public static int countCompactors(ResourceGroupId group, ClientContext context) {
    var start = Timer.startNew();
    int count = context.getServerPaths()
        .getCompactor(ResourceGroupPredicate.exact(group), AddressSelector.all(), true).size();
    long elapsed = start.elapsed(MILLISECONDS);
    if (elapsed > 100) {
      LOG.debug("Took {} ms to count {} compactors for {}", elapsed, count, group);
    } else {
      LOG.trace("Took {} ms to count {} compactors for {}", elapsed, count, group);
    }
    return count;
  }

  public static void cancelCompaction(ClientContext context, HostAndPort compactorAddr,
      String ecid) {
    CompactorService.Client client = null;
    try {
      client = ThriftUtil.getClient(ThriftClientTypes.COMPACTOR, compactorAddr, context);
      client.cancel(TraceUtil.traceInfo(), context.rpcCreds(), ecid);
    } catch (TException e) {
      LOG.debug("Failed to cancel compactor {} for {}", compactorAddr, ecid, e);
    } finally {
      ThriftUtil.returnClient(client, context);
    }
  }

  public static Optional<HostAndPort> findCompactorRunningCompaction(ClientContext context,
      ExternalCompactionId ecid) {
    for (var level : Ample.DataLevel.values()) {
      var compactor = findCompactorRunningCompaction(context, level, ecid);
      if (compactor.isPresent()) {
        return compactor;
      }
    }

    return Optional.empty();
  }

  private static Optional<HostAndPort> findCompactorRunningCompaction(ClientContext context,
      Ample.DataLevel level, ExternalCompactionId ecid) {
    try (var tablets = context.getAmple().readTablets().forLevel(level).fetch(ECOMP).build()) {
      Optional<Map.Entry<ExternalCompactionId,CompactionMetadata>> ecomp =
          tablets.stream().flatMap(tm -> tm.getExternalCompactions().entrySet().stream())
              .filter(e -> e.getKey().equals(ecid)).findFirst();
      return ecomp.map(entry -> HostAndPort.fromString(entry.getValue().getCompactorId()));
    } catch (Exception e) {
      throw new IllegalStateException("Exception calling cancel compaction for " + ecid, e);
    }
  }
}
