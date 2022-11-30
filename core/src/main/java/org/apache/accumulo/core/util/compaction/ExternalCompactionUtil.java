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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.compaction.thrift.CompactorService;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooSession;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalCompactionUtil {

  private static class RunningCompactionFuture {
    private final String queue;
    private final HostAndPort compactor;
    private final Future<TExternalCompactionJob> future;

    public RunningCompactionFuture(String queue, HostAndPort compactor,
        Future<TExternalCompactionJob> future) {
      this.queue = queue;
      this.compactor = compactor;
      this.future = future;
    }

    public String getQueue() {
      return queue;
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
    final String lockPath = context.getZooKeeperRoot() + Constants.ZCOORDINATOR_LOCK;
    try {
      var zk = ZooSession.getAnonymousSession(context.getZooKeepers(),
          context.getZooKeepersSessionTimeOut());
      byte[] address = ServiceLock.getLockData(zk, ServiceLock.path(lockPath));
      if (null == address) {
        return Optional.empty();
      }
      return Optional.of(HostAndPort.fromString(new String(address)));
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return map of queue names to compactor addresses
   */
  public static Map<String,List<HostAndPort>> getCompactorAddrs(ClientContext context) {
    try {
      final Map<String,List<HostAndPort>> queuesAndAddresses = new HashMap<>();
      final String compactorQueuesPath = context.getZooKeeperRoot() + Constants.ZCOMPACTORS;
      ZooReader zooReader = context.getZooReader();
      List<String> queues = zooReader.getChildren(compactorQueuesPath);
      for (String queue : queues) {
        queuesAndAddresses.putIfAbsent(queue, new ArrayList<HostAndPort>());
        try {
          List<String> compactors = zooReader.getChildren(compactorQueuesPath + "/" + queue);
          for (String compactor : compactors) {
            // compactor is the address, we are checking to see if there is a child node which
            // represents the compactor's lock as a check that it's alive.
            List<String> children =
                zooReader.getChildren(compactorQueuesPath + "/" + queue + "/" + compactor);
            if (!children.isEmpty()) {
              LOG.trace("Found live compactor {} ", compactor);
              queuesAndAddresses.get(queue).add(HostAndPort.fromString(compactor));
            }
          }
        } catch (NoNodeException e) {
          LOG.trace("Ignoring node that went missing", e);
        }
      }

      return queuesAndAddresses;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
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
   * @return map of compactor and external compaction jobs
   */
  public static List<RunningCompaction> getCompactionsRunningOnCompactors(ClientContext context) {
    final List<RunningCompactionFuture> rcFutures = new ArrayList<>();
    final ExecutorService executor = ThreadPools.getServerThreadPools().createFixedThreadPool(16,
        "CompactorRunningCompactions", false);

    getCompactorAddrs(context).forEach((q, hp) -> {
      hp.forEach(hostAndPort -> {
        rcFutures.add(new RunningCompactionFuture(q, hostAndPort,
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
          results.add(new RunningCompaction(job, compactorAddress, rcf.getQueue()));
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    return results;
  }

  public static Collection<ExternalCompactionId>
      getCompactionIdsRunningOnCompactors(ClientContext context) {
    final ExecutorService executor = ThreadPools.getServerThreadPools().createFixedThreadPool(16,
        "CompactorRunningCompactions", false);

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
        throw new RuntimeException(e);
      }
    });

    return runningIds;
  }

  public static int countCompactors(String queueName, ClientContext context) {
    String queueRoot = context.getZooKeeperRoot() + Constants.ZCOMPACTORS + "/" + queueName;
    List<String> children = context.getZooCache().getChildren(queueRoot);
    if (children == null) {
      return 0;
    }

    int count = 0;

    for (String child : children) {
      List<String> children2 = context.getZooCache().getChildren(queueRoot + "/" + child);
      if (children2 != null && !children2.isEmpty()) {
        count++;
      }
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
