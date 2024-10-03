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
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.COMPACTOR_RUNNING_COMPACTIONS_POOL;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.COMPACTOR_RUNNING_COMPACTION_IDS_POOL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.compaction.thrift.CompactorService;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class ExternalCompactionUtil {

  private static class RunningCompactionFuture {
    private final String group;
    private final HostAndPort compactor;
    private final Future<TExternalCompactionJob> future;

    public RunningCompactionFuture(String group, HostAndPort compactor,
        Future<TExternalCompactionJob> future) {
      this.group = group;
      this.compactor = compactor;
      this.future = future;
    }

    public String getGroup() {
      return group;
    }

    public HostAndPort getCompactor() {
      return compactor;
    }

    public Future<TExternalCompactionJob> getFuture() {
      return future;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ExternalCompactionUtil.class);

  /**
   * Utility for returning the address of a service in the form host:port
   *
   * @param address HostAndPort of service
   * @return host and port
   */
  public static String getHostPortString(HostAndPort address) {
    if (address == null) {
      return null;
    }
    return address.toString();
  }

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
   * @return map of group names to compactor addresses
   */
  public static Map<String,Set<HostAndPort>> getCompactorAddrs(ClientContext context) {
    final Map<String,Set<HostAndPort>> groupsAndAddresses = new HashMap<>();
    context.getServerPaths().getCompactor(Optional.empty(), Optional.empty(), true).forEach(slp -> {
      groupsAndAddresses.computeIfAbsent(slp.getResourceGroup(), (k) -> new HashSet<>())
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
  public static TExternalCompactionJob getRunningCompaction(HostAndPort compactorAddr,
      ClientContext context) {

    CompactorService.Client client = null;
    try {
      client = ThriftUtil.getClient(ThriftClientTypes.COMPACTOR, compactorAddr, context);
      TExternalCompactionJob job =
          client.getRunningCompaction(TraceUtil.traceInfo(), context.rpcCreds());
      if (job.getExternalCompactionId() != null) {
        LOG.debug("Compactor {} is running {}", compactorAddr, job.getExternalCompactionId());
        return job;
      }
    } catch (TException e) {
      LOG.debug("Failed to contact compactor {}", compactorAddr, e);
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
   * This method returns information from the Compactor about the job that is currently running. The
   * RunningCompactions are not fully populated. This method is used from the CompactionCoordinator
   * on a restart to re-populate the set of running compactions on the compactors.
   *
   * @param context server context
   * @return list of compactor and external compaction jobs
   */
  public static List<RunningCompaction> getCompactionsRunningOnCompactors(ClientContext context) {
    final List<RunningCompactionFuture> rcFutures = new ArrayList<>();
    final ExecutorService executor = ThreadPools.getServerThreadPools()
        .getPoolBuilder(COMPACTOR_RUNNING_COMPACTIONS_POOL).numCoreThreads(16).build();

    getCompactorAddrs(context).forEach((group, hp) -> {
      hp.forEach(hostAndPort -> {
        rcFutures.add(new RunningCompactionFuture(group, hostAndPort,
            executor.submit(() -> getRunningCompaction(hostAndPort, context))));
      });
    });
    executor.shutdown();

    final List<RunningCompaction> results = new ArrayList<>();
    rcFutures.forEach(rcf -> {
      try {
        TExternalCompactionJob job = rcf.getFuture().get();
        if (null != job && null != job.getExternalCompactionId()) {
          var compactorAddress = getHostPortString(rcf.getCompactor());
          results.add(new RunningCompaction(job, compactorAddress, rcf.getGroup()));
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new IllegalStateException(e);
      }
    });
    return results;
  }

  public static Collection<ExternalCompactionId>
      getCompactionIdsRunningOnCompactors(ClientContext context) {
    final ExecutorService executor = ThreadPools.getServerThreadPools()
        .getPoolBuilder(COMPACTOR_RUNNING_COMPACTION_IDS_POOL).numCoreThreads(16).build();
    List<Future<ExternalCompactionId>> futures = new ArrayList<>();

    getCompactorAddrs(context).forEach((q, hp) -> {
      hp.forEach(hostAndPort -> {
        futures.add(executor.submit(() -> getRunningCompactionId(hostAndPort, context)));
      });
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

  public static int countCompactors(String groupName, ClientContext context) {
    var start = Timer.startNew();
    int count = context.getServerPaths()
        .getCompactor(Optional.of(groupName), Optional.empty(), true).size();
    long elapsed = start.elapsed(MILLISECONDS);
    if (elapsed > 100) {
      LOG.debug("Took {} ms to count {} compactors for {}", elapsed, count, groupName);
    } else {
      LOG.trace("Took {} ms to count {} compactors for {}", elapsed, count, groupName);
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
}
