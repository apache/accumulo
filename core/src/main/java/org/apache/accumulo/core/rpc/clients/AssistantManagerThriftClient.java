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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.ThriftConcurrentModificationException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.manager.thrift.AssistantManagerClientService;
import org.apache.accumulo.core.manager.thrift.AssistantManagerClientService.Client;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class AssistantManagerThriftClient extends ThriftClientTypes<Client>
    implements TServerClient<Client> {

  private static final Logger LOG = LoggerFactory.getLogger(AssistantManagerThriftClient.class);
  private final AtomicBoolean warnedAboutAssistantManagersBeingDown = new AtomicBoolean(false);

  AssistantManagerThriftClient(String serviceName) {
    super(serviceName, new Client.Factory());
  }

  @Override
  public Client getConnection(ClientContext context) {
    return getThriftServerConnection(context, false).getSecond();
  }

  @Override
  public Pair<String,Client> getThriftServerConnection(ClientContext context,
      boolean preferCachedConnections) {
    try {
      return getThriftServerConnection(LOG, this, context, preferCachedConnections,
          warnedAboutAssistantManagersBeingDown, ThriftService.FATE_WORKER);
    } catch (TTransportException tte) {
      Throwable cause = tte.getCause();
      if (cause instanceof UnknownHostException) {
        // do not expect to recover from this
        throw new IllegalStateException(tte);
      }
      LOG.debug("Failed to connect to a assistant manager, will retry... ", tte);
      return null;
    }
  }

  public Pair<String,Client> getThriftServerConnection(Logger LOG, ThriftClientTypes<Client> type,
      ClientContext context, boolean preferCachedConnections, AtomicBoolean warned,
      ThriftService service) throws TTransportException {
    checkArgument(context != null, "context is null");
    final long rpcTimeout = context.getClientTimeoutInMillis();
    final ZooCache zc = context.getZooCache();
    final String debugHost = System.getProperty(DEBUG_HOST, null);
    final boolean debugHostSpecified = debugHost != null;

    ArrayList<ServiceLockPath> assistManagerPaths =
        new ArrayList<>(context.getServerPaths().getAssistantManagers(AddressSelector.all(), true));
    Collections.shuffle(assistManagerPaths, RANDOM.get());

    for (ServiceLockPath path : assistManagerPaths) {
      Optional<ServiceLockData> data = zc.getLockData(path);
      if (data != null && data.isPresent()) {
        HostAndPort assistManagerClientAddress = data.orElseThrow().getAddress(service);
        if (assistManagerClientAddress != null) {
          try {
            TTransport transport = context.getTransportPool().getTransport(type,
                assistManagerClientAddress, rpcTimeout, context, preferCachedConnections);
            AssistantManagerClientService.Client client =
                ThriftUtil.createClient(type, transport, context.getInstanceID());
            if (debugHostSpecified) {
              LOG.info("Connecting to debug host: {}", debugHost);
            }
            warned.set(false);
            return new Pair<>(assistManagerClientAddress.toString(), client);
          } catch (TTransportException e) {
            if (debugHostSpecified) {
              LOG.error(
                  "Error creating transport to debug host: {}. If this server is"
                      + " down, then you will need to remove or change the system property {}.",
                  debugHost, DEBUG_HOST);
            } else {
              LOG.trace("Error creating transport to {}", assistManagerClientAddress);
            }
          }
        }
      }
    }

    if (warned.compareAndSet(false, true)) {
      LOG.warn("Failed to find an available server in the list of servers: {} for API type: {}",
          assistManagerPaths, type);
    }
    // Need to throw a different exception, when a TTransportException is
    // thrown below, then the operation will be retried endlessly.
    if (debugHostSpecified) {
      throw new UncheckedIOException("Error creating transport to debug host: " + debugHost
          + ". If this server is down, then you will need to remove or change the system property "
          + DEBUG_HOST + ".", new IOException(""));
    } else {
      throw new TTransportException("Failed to connect to any server for API type " + type);
    }
  }

  public <R> R executeTableCommand(ClientContext context, Exec<R,Client> exec)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Client client = null;
    while (true) {
      try {
        client = getConnectionWithRetry(context);
        return exec.execute(client);
      } catch (TTransportException tte) {
        LOG.debug("ManagerClient request failed, retrying ... ", tte);
        sleepUninterruptibly(100, MILLISECONDS);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (ThriftTableOperationException e) {
        switch (e.getType()) {
          case NAMESPACE_NOTFOUND:
            throw new TableNotFoundException(e.getTableName(), new NamespaceNotFoundException(e));
          case NOTFOUND:
            throw new TableNotFoundException(e);
          default:
            throw new AccumuloException(e);
        }
      } catch (ThriftNotActiveServiceException e) {
        // Let it loop, fetching a new location
        LOG.debug("Contacted a Manager which is no longer active, retrying");
        sleepUninterruptibly(100, MILLISECONDS);
      } catch (ThriftConcurrentModificationException e) {
        throw new ConcurrentModificationException(e.getMessage(), e);
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null) {
          ThriftUtil.close(client, context);
        }
      }
    }
  }

  @Override
  public <R> R execute(ClientContext context, Exec<R,Client> exec)
      throws AccumuloException, AccumuloSecurityException {
    try {
      return executeTableCommand(context, exec);
    } catch (TableNotFoundException e) {
      throw new AssertionError(e);
    }
  }

  public void executeVoidTableCommand(ClientContext context, ExecVoid<Client> exec)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Client client = null;
    while (true) {
      try {
        client = getConnectionWithRetry(context);
        exec.execute(client);
        return;
      } catch (TTransportException tte) {
        LOG.debug("ManagerClient request failed, retrying ... ", tte);
        sleepUninterruptibly(100, MILLISECONDS);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (ThriftTableOperationException e) {
        switch (e.getType()) {
          case NAMESPACE_NOTFOUND:
            throw new TableNotFoundException(e.getTableName(), new NamespaceNotFoundException(e));
          case NOTFOUND:
            throw new TableNotFoundException(e);
          default:
            throw new AccumuloException(e);
        }
      } catch (ThriftNotActiveServiceException e) {
        // Let it loop, fetching a new location
        LOG.debug("Contacted a Manager which is no longer active, retrying");
        sleepUninterruptibly(100, MILLISECONDS);
      } catch (ThriftConcurrentModificationException e) {
        throw new ConcurrentModificationException(e.getMessage(), e);
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null) {
          ThriftUtil.close(client, context);
        }
      }
    }
  }

  @Override
  public void executeVoid(ClientContext context, ExecVoid<Client> exec)
      throws AccumuloException, AccumuloSecurityException {
    try {
      executeVoidTableCommand(context, exec);
    } catch (TableNotFoundException e) {
      throw new AccumuloException(e);
    }
  }
}
