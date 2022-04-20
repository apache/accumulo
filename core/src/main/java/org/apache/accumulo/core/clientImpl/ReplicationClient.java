/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.clientImpl;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.rpc.ThriftClientTypes;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationClient {
  private static final Logger log = LoggerFactory.getLogger(ReplicationClient.class);


  /**
   * Attempt a single time to create a ReplicationServicer client to the given host
   *
   * @param context
   *          The client session for the peer replicant
   * @param server
   *          Server to connect to
   * @param timeout
   *          RPC timeout in milliseconds
   * @return A ReplicationServicer client to the given host in the given instance
   */
  public static ReplicationServicer.Client getServicerConnection(ClientContext context,
      HostAndPort server, long timeout) throws TTransportException {
    requireNonNull(context);
    requireNonNull(server);

    try {
      return ThriftUtil.getClient(ThriftClientTypes.REPLICATION_SERVICER, server, context, timeout);
    } catch (TTransportException tte) {
      log.debug("Failed to connect to servicer ({}), will retry...", server, tte);
      throw tte;
    }
  }

  public static <T> T executeCoordinatorWithReturn(ClientContext context,
      ClientExecReturn<T,ReplicationCoordinator.Client> exec)
      throws AccumuloException, AccumuloSecurityException {
    ReplicationCoordinator.Client client = null;
    for (int i = 0; i < 10; i++) {
      try {
        client = ThriftClientTypes.REPLICATION_COORDINATOR.getManagerConnectionWithRetry(context);
        return exec.execute(client);
      } catch (TTransportException tte) {
        log.debug("ReplicationClient coordinator request failed, retrying ... ", tte);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new AccumuloException(e);
        }
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (AccumuloException e) {
        throw e;
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          ThriftUtil.close(client, context);
      }
    }

    throw new AccumuloException(
        "Could not connect to ReplicationCoordinator at " + context.getInstanceName());
  }

  public static <T> T executeServicerWithReturn(ClientContext context, HostAndPort tserver,
      ClientExecReturn<T,ReplicationServicer.Client> exec, long timeout)
      throws AccumuloException, AccumuloSecurityException {
    ReplicationServicer.Client client = null;
    try {
      client = getServicerConnection(context, tserver, timeout);
      return exec.execute(client);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (AccumuloException e) {
      throw e;
    } catch (Exception e) {
      throw new AccumuloException(e);
    } finally {
      if (client != null)
        ThriftUtil.close(client, context);
    }
  }

}
