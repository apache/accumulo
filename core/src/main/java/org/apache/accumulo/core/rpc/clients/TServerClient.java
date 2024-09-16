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
package org.apache.accumulo.core.rpc.clients;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.AccumuloServerException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes.Exec;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes.ExecVoid;
import org.apache.accumulo.core.util.Pair;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;

import com.google.common.net.HostAndPort;

public interface TServerClient<C extends TServiceClient> {

  Pair<String,C> getThriftServerConnection(ClientContext context, boolean preferCachedConnections)
      throws TTransportException;

  default Pair<String,C> getThriftServerConnection(Logger LOG, ThriftClientTypes<C> type,
      ClientContext context, boolean preferCachedConnections, AtomicBoolean warned,
      ThriftService service) throws TTransportException {
    checkArgument(context != null, "context is null");

    if (preferCachedConnections) {
      Pair<String,TTransport> cachedTransport =
          context.getTransportPool().getAnyCachedTransport(type);
      if (cachedTransport != null) {
        C client = ThriftUtil.createClient(type, cachedTransport.getSecond());
        warned.set(false);
        return new Pair<String,C>(cachedTransport.getFirst(), client);
      }
    }

    final long rpcTimeout = context.getClientTimeoutInMillis();
    final ZooCache zc = context.getZooCache();
    final List<ServiceLockPath> serverPaths = new ArrayList<>();
    serverPaths
        .addAll(context.getServerPaths().getCompactor(Optional.empty(), Optional.empty(), true));
    serverPaths
        .addAll(context.getServerPaths().getScanServer(Optional.empty(), Optional.empty(), true));
    serverPaths
        .addAll(context.getServerPaths().getTabletServer(Optional.empty(), Optional.empty(), true));
    if (serverPaths.isEmpty()) {
      if (warned.compareAndSet(false, true)) {
        LOG.warn(
            "There are no servers serving the {} api: check that zookeeper and accumulo are running.",
            type);
      }
      throw new TTransportException("There are no servers for type: " + type);
    }
    Collections.shuffle(serverPaths, RANDOM.get());

    for (ServiceLockPath path : serverPaths) {
      Optional<ServiceLockData> data = zc.getLockData(path);
      if (data != null && data.isPresent()) {
        HostAndPort tserverClientAddress = data.orElseThrow().getAddress(service);
        if (tserverClientAddress != null) {
          try {
            TTransport transport = context.getTransportPool().getTransport(type,
                tserverClientAddress, rpcTimeout, context, preferCachedConnections);
            C client = ThriftUtil.createClient(type, transport);
            warned.set(false);
            return new Pair<String,C>(tserverClientAddress.toString(), client);
          } catch (TTransportException e) {
            LOG.trace("Error creating transport to {}", tserverClientAddress);
            continue;
          }
        }
      }
    }

    if (warned.compareAndSet(false, true)) {
      LOG.warn("Failed to find an available server in the list of servers: {} for API type: {}",
          serverPaths, type);
    }
    throw new TTransportException("Failed to connect to any server for API type " + type);
  }

  default <R> R execute(Logger LOG, ClientContext context, Exec<R,C> exec)
      throws AccumuloException, AccumuloSecurityException {
    while (true) {
      String server = null;
      C client = null;
      try {
        Pair<String,C> pair = getThriftServerConnection(context, true);
        server = pair.getFirst();
        client = pair.getSecond();
        return exec.execute(client);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (TApplicationException tae) {
        throw new AccumuloServerException(server, tae);
      } catch (TTransportException tte) {
        LOG.debug("ClientService request failed " + server + ", retrying ... ", tte);
        sleepUninterruptibly(100, MILLISECONDS);
      } catch (TException e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null) {
          ThriftUtil.close(client, context);
        }
      }
    }
  }

  default void executeVoid(Logger LOG, ClientContext context, ExecVoid<C> exec)
      throws AccumuloException, AccumuloSecurityException {
    while (true) {
      String server = null;
      C client = null;
      try {
        Pair<String,C> pair = getThriftServerConnection(context, true);
        server = pair.getFirst();
        client = pair.getSecond();
        exec.execute(client);
        return;
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (TApplicationException tae) {
        throw new AccumuloServerException(server, tae);
      } catch (TTransportException tte) {
        LOG.debug("ClientService request failed " + server + ", retrying ... ", tte);
        sleepUninterruptibly(100, MILLISECONDS);
      } catch (TException e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null) {
          ThriftUtil.close(client, context);
        }
      }
    }
  }

}
