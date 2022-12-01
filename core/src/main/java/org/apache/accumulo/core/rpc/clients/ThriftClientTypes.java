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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;

public abstract class ThriftClientTypes<C extends TServiceClient> {

  public static final ClientServiceThriftClient CLIENT = new ClientServiceThriftClient("client");

  public static final CompactorServiceThriftClient COMPACTOR =
      new CompactorServiceThriftClient("compactor");

  public static final CompactionCoordinatorServiceThriftClient COORDINATOR =
      new CompactionCoordinatorServiceThriftClient("coordinator");

  public static final FateThriftClient FATE = new FateThriftClient("fate");

  public static final GCMonitorServiceThriftClient GC = new GCMonitorServiceThriftClient("gc");

  public static final ManagerThriftClient MANAGER = new ManagerThriftClient("mgr");

  public static final TabletServerThriftClient TABLET_SERVER =
      new TabletServerThriftClient("tablet");

  public static final TabletScanClientServiceThriftClient TABLET_SCAN =
      new TabletScanClientServiceThriftClient("scan");

  /**
   * execute method with supplied client returning object of type R
   *
   * @param <R> return type
   * @param <C> client type
   */
  public interface Exec<R,C> {
    R execute(C client) throws TException;
  }

  /**
   * execute method with supplied client
   *
   * @param <C> client type
   */
  public interface ExecVoid<C> {
    void execute(C client) throws TException;
  }

  private final String serviceName;
  private final TServiceClientFactory<C> clientFactory;

  public ThriftClientTypes(String serviceName, TServiceClientFactory<C> factory) {
    this.serviceName = serviceName;
    this.clientFactory = factory;
  }

  public final String getServiceName() {
    return serviceName;
  }

  public final TServiceClientFactory<C> getClientFactory() {
    return clientFactory;
  }

  public C getClient(TProtocol prot) {
    // All server side TProcessors are multiplexed. Wrap this protocol.
    return getClientFactory().getClient(new TMultiplexedProtocol(prot, getServiceName()));
  }

  public C getConnection(ClientContext context) {
    throw new UnsupportedOperationException("This method has not been implemented");
  }

  public C getConnectionWithRetry(ClientContext context) {
    while (true) {
      C result = getConnection(context);
      if (result != null) {
        return result;
      }
      sleepUninterruptibly(250, MILLISECONDS);
    }
  }

  public <R> R execute(ClientContext context, Exec<R,C> exec)
      throws AccumuloException, AccumuloSecurityException {
    throw new UnsupportedOperationException("This method has not been implemented");
  }

  public void executeVoid(ClientContext context, ExecVoid<C> exec)
      throws AccumuloException, AccumuloSecurityException {
    throw new UnsupportedOperationException("This method has not been implemented");
  }

}
