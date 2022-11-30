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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.util.ConcurrentModificationException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.ThriftConcurrentModificationException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.manager.thrift.ManagerClientService.Client;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagerThriftClient extends ThriftClientTypes<Client>
    implements ManagerClient<Client> {

  private static final Logger LOG = LoggerFactory.getLogger(ManagerThriftClient.class);

  ManagerThriftClient(String serviceName) {
    super(serviceName, new Client.Factory());
  }

  @Override
  public Client getConnection(ClientContext context) {
    return getManagerConnection(LOG, this, context);
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
