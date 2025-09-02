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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.AccumuloServerException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes.Exec;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes.ExecVoid;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;

import com.google.common.net.HostAndPort;

public interface TServerClient<C extends TServiceClient> {

  static final String DEBUG_HOST = "org.apache.accumulo.client.rpc.debug.host";
  static final String DEBUG_RG = "org.apache.accumulo.client.rpc.debug.group";

  Pair<String,C> getThriftServerConnection(ClientContext context, boolean preferCachedConnections,
      ResourceGroupPredicate rgp) throws TTransportException;

  default Pair<String,C> getThriftServerConnection(Logger LOG, ThriftClientTypes<C> type,
      ClientContext context, boolean preferCachedConnections, AtomicBoolean warned,
      ThriftService service, ResourceGroupPredicate rgp) throws TTransportException {
    checkArgument(context != null, "context is null");

    final String debugHost = System.getProperty(DEBUG_HOST, null);
    final boolean debugHostSpecified = debugHost != null;
    final String debugRG = System.getProperty(DEBUG_RG, null);
    final boolean debugRGSpecified = debugRG != null;
    final ResourceGroupId debugRGid = debugRGSpecified ? ResourceGroupId.of(debugRG) : null;

    if (debugHostSpecified && debugRGSpecified) {
      LOG.warn("System properties {} and {} are both set. If set incorrectly then"
          + " this client may not find a server to connect to.", DEBUG_HOST, DEBUG_RG);
    }

    if (debugRGSpecified) {
      if (type == ThriftClientTypes.CLIENT || type == ThriftClientTypes.COMPACTOR
          || type == ThriftClientTypes.SERVER_PROCESS || type == ThriftClientTypes.TABLET_INGEST
          || type == ThriftClientTypes.TABLET_MGMT || type == ThriftClientTypes.TABLET_SCAN
          || type == ThriftClientTypes.TABLET_SERVER) {
        if (rgp.test(debugRGid)) {
          // its safe to potentially narrow the predicate
          LOG.debug("System property '{}' set to '{}' overriding predicate argument", DEBUG_RG,
              debugRG);
          rgp = ResourceGroupPredicate.exact(debugRGid);
        } else {
          LOG.warn("System property '{}' set to '{}' does not intersect with predicate argument."
              + " Ignoring degug system property.", DEBUG_RG, debugRG);
        }
      } else {
        LOG.debug(
            "System property '{}' set to '{}' but ignored when making RPCs to management servers",
            DEBUG_RG, debugRG);
      }
    }

    if (preferCachedConnections && !debugHostSpecified && !debugRGSpecified) {
      Pair<String,TTransport> cachedTransport =
          context.getTransportPool().getAnyCachedTransport(type, context, service, rgp);
      if (cachedTransport != null) {
        C client = ThriftUtil.createClient(type, cachedTransport.getSecond());
        warned.set(false);
        return new Pair<String,C>(cachedTransport.getFirst(), client);
      }
    }

    final long rpcTimeout = context.getClientTimeoutInMillis();
    final ZooCache zc = context.getZooCache();
    final ServiceLockPaths sp = context.getServerPaths();
    final List<ServiceLockPath> serverPaths = new ArrayList<>();

    if (type == ThriftClientTypes.CLIENT && debugHostSpecified) {
      // add all three paths to the set even though they may not be correct.
      // The entire set will be checked in the code below to validate
      // that the path is correct and the lock is held and will return the
      // correct one.
      HostAndPort hp = HostAndPort.fromString(debugHost);
      serverPaths.addAll(sp.getCompactor(rgp, AddressSelector.exact(hp), true));
      serverPaths.addAll(sp.getScanServer(rgp, AddressSelector.exact(hp), true));
      serverPaths.addAll(sp.getTabletServer(rgp, AddressSelector.exact(hp), true));
    } else {
      serverPaths.addAll(sp.getTabletServer(rgp, AddressSelector.all(), false));
      if (type == ThriftClientTypes.CLIENT) {
        serverPaths.addAll(sp.getCompactor(rgp, AddressSelector.all(), false));
        serverPaths.addAll(sp.getScanServer(rgp, AddressSelector.all(), false));
      }
      if (serverPaths.isEmpty()) {
        if (warned.compareAndSet(false, true)) {
          LOG.warn(
              "There are no servers serving the {} api: check that zookeeper and accumulo are running.",
              type);
        }
        // If the user set the system property for the resource group, then don't throw
        // a TTransportException here. That will cause the call to be continuously retried.
        // Instead, let this continue so that we can throw a different error below.
        if (!debugRGSpecified) {
          throw new TTransportException("There are no servers for type: " + type);
        }
      }
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
            if (type == ThriftClientTypes.CLIENT && debugHostSpecified) {
              LOG.info("Connecting to debug host: {}", debugHost);
            }
            warned.set(false);
            return new Pair<String,C>(tserverClientAddress.toString(), client);
          } catch (TTransportException e) {
            if (type == ThriftClientTypes.CLIENT && debugHostSpecified) {
              LOG.error(
                  "Error creating transport to debug host: {}. If this server is"
                      + " down, then you will need to remove or change the system property {}.",
                  debugHost, DEBUG_HOST);
            } else if (debugRGSpecified && rgp.test(debugRGid)) {
              LOG.error(
                  "Error creating transport to debug group: {}. If all servers are"
                      + " down, then you will need to remove or change the system property {}.",
                  debugRG, DEBUG_RG);
            } else {
              LOG.trace("Error creating transport to {}", tserverClientAddress);
            }
            continue;
          }
        }
      }
    }

    if (warned.compareAndSet(false, true)) {
      LOG.warn("Failed to find an available server in the list of servers: {} for API type: {}",
          serverPaths, type);
    }
    // Need to throw a different exception, when a TTransportException is
    // thrown below, then the operation will be retried endlessly.
    if (type == ThriftClientTypes.CLIENT && debugHostSpecified) {
      throw new UncheckedIOException("Error creating transport to debug host: " + debugHost
          + ". If this server is down, then you will need to remove or change the system property "
          + DEBUG_HOST + ".", new IOException(""));
    } else if (debugRGSpecified && rgp.test(debugRGid)) {
      throw new UncheckedIOException("Error creating transport to debug group: " + debugRG
          + ". If all servers are down, then you will need to remove or change the system property "
          + DEBUG_RG + ".", new IOException(""));
    } else {
      throw new TTransportException("Failed to connect to any server for API type " + type);
    }
  }

  default <R> R execute(Logger LOG, ClientContext context, Exec<R,C> exec,
      ResourceGroupPredicate rgp) throws AccumuloException, AccumuloSecurityException {
    while (true) {
      String server = null;
      C client = null;
      try {
        Pair<String,C> pair = getThriftServerConnection(context, true, rgp);
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
      } catch (ThriftTableOperationException ttoe) {
        TableNotFoundException tnfe;
        switch (ttoe.getType()) {
          case NOTFOUND:
            tnfe = new TableNotFoundException(ttoe);
            throw new AccumuloException(tnfe);
          case NAMESPACE_NOTFOUND:
            tnfe = new TableNotFoundException(ttoe.getTableName(),
                new NamespaceNotFoundException(ttoe));
            throw new AccumuloException(tnfe);
          default:
            throw new AccumuloException(ttoe);
        }
      } catch (TException e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null) {
          ThriftUtil.close(client, context);
        }
      }
    }
  }

  default void executeVoid(Logger LOG, ClientContext context, ExecVoid<C> exec,
      ResourceGroupPredicate rgp) throws AccumuloException, AccumuloSecurityException {
    while (true) {
      String server = null;
      C client = null;
      try {
        Pair<String,C> pair = getThriftServerConnection(context, true, rgp);
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
      } catch (ThriftTableOperationException ttoe) {
        TableNotFoundException tnfe;
        switch (ttoe.getType()) {
          case NOTFOUND:
            tnfe = new TableNotFoundException(ttoe);
            throw new AccumuloException(tnfe);
          case NAMESPACE_NOTFOUND:
            tnfe = new TableNotFoundException(ttoe.getTableName(),
                new NamespaceNotFoundException(ttoe));
            throw new AccumuloException(tnfe);
          default:
            throw new AccumuloException(ttoe);
        }
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
