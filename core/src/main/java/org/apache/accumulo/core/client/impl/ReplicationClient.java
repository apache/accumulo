/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.client.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.net.UnknownHostException;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationClient {
  private static final Logger log = LoggerFactory.getLogger(ReplicationClient.class);

  /**
   * @param instance
   *          Instance for the peer replicant
   * @return Client to the ReplicationCoordinator service
   */
  public static ReplicationCoordinator.Client getCoordinatorConnectionWithRetry(Instance instance) {
    checkArgument(instance != null, "instance is null");

    while (true) {

      ReplicationCoordinator.Client result = getCoordinatorConnection(instance);
      if (result != null)
        return result;
      UtilWaitThread.sleep(250);
    }

  }

  public static ReplicationCoordinator.Client getCoordinatorConnection(Instance instance) {
    List<String> locations = instance.getMasterLocations();

    if (locations.size() == 0) {
      log.debug("No masters...");
      return null;
    }

    String master = locations.get(0);
    if (master.endsWith(":0"))
      return null;

    try {
      // Master requests can take a long time: don't ever time out
      ReplicationCoordinator.Client client = ThriftUtil.getClientNoTimeout(new ReplicationCoordinator.Client.Factory(), master,
          ServerConfigurationUtil.getConfiguration(instance));
      return client;
    } catch (TTransportException tte) {
      if (tte.getCause().getClass().equals(UnknownHostException.class)) {
        // do not expect to recover from this
        throw new RuntimeException(tte);
      }
      log.debug("Failed to connect to master={}, will retry... ", master, tte);
      return null;
    }
  }

  /**
   * Attempt a single time to create a ReplicationServicer client to the given host
   * 
   * @param inst
   *          Instance to the peer replicant
   * @param server
   *          Server to connect to
   * @return A ReplicationServicer client to the given host in the given instance
   */
  public static ReplicationServicer.Client getServicerConnection(Instance inst, String server) throws TTransportException {
    checkNotNull(inst);
    checkNotNull(server);

    try {
      return ThriftUtil.getClientNoTimeout(new ReplicationServicer.Client.Factory(), server, ServerConfigurationUtil.getConfiguration(inst));
    } catch (TTransportException tte) {
      log.debug("Failed to connect to servicer ({}), will retry...", tte);
      throw tte;
    }
  }

  public static void close(ReplicationCoordinator.Iface iface) {
    TServiceClient client = (TServiceClient) iface;
    if (client != null && client.getInputProtocol() != null && client.getInputProtocol().getTransport() != null) {
      ThriftTransportPool.getInstance().returnTransport(client.getInputProtocol().getTransport());
    } else {
      log.debug("Attempt to close null connection to the remote system", new Exception());
    }
  }

  public static void close(ReplicationServicer.Iface iface) {
    TServiceClient client = (TServiceClient) iface;
    if (client != null && client.getInputProtocol() != null && client.getInputProtocol().getTransport() != null) {
      ThriftTransportPool.getInstance().returnTransport(client.getInputProtocol().getTransport());
    } else {
      log.debug("Attempt to close null connection to the remote system", new Exception());
    }
  }

  public static <T> T executeCoordinatorWithReturn(Instance instance, ClientExecReturn<T,ReplicationCoordinator.Client> exec) throws AccumuloException,
      AccumuloSecurityException {
    ReplicationCoordinator.Client client = null;
    while (true) {
      try {
        client = getCoordinatorConnectionWithRetry(instance);
        return exec.execute(client);
      } catch (TTransportException tte) {
        log.debug("ReplicationClient coordinator request failed, retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (AccumuloException e) {
        throw e;
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client);
      }
    }
  }

  public static void executeCoordinator(Instance instance, ClientExec<ReplicationCoordinator.Client> exec) throws AccumuloException, AccumuloSecurityException {
    ReplicationCoordinator.Client client = null;
    try {
      client = getCoordinatorConnectionWithRetry(instance);
      exec.execute(client);
    } catch (TTransportException tte) {
      log.debug("ReplicationClient coordinator request failed, retrying ... ", tte);
      UtilWaitThread.sleep(100);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (AccumuloException e) {
      throw e;
    } catch (Exception e) {
      throw new AccumuloException(e);
    } finally {
      if (client != null)
        close(client);
    }
  }

  public static <T> T executeServicerWithReturn(Instance instance, String tserver, ClientExecReturn<T,ReplicationServicer.Client> exec)
      throws AccumuloException, AccumuloSecurityException, TTransportException {
    ReplicationServicer.Client client = null;
    while (true) {
      try {
        client = getServicerConnection(instance, tserver);
        return exec.execute(client);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (AccumuloException e) {
        throw e;
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client);
      }
    }
  }

  public static void executeServicer(Instance instance, String tserver, ClientExec<ReplicationServicer.Client> exec) throws AccumuloException,
      AccumuloSecurityException, TTransportException {
    ReplicationServicer.Client client = null;
    try {
      client = getServicerConnection(instance, tserver);
      exec.execute(client);
      return;
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (AccumuloException e) {
      throw e;
    } catch (Exception e) {
      throw new AccumuloException(e);
    } finally {
      if (client != null)
        close(client);
    }
  }

}
