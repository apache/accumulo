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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.client.replication.PeerExistsException;
import org.apache.accumulo.core.client.replication.PeerNotFoundException;
import org.apache.accumulo.core.client.replication.ReplicaSystem;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService.Client;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.trace.instrument.Tracer;

/**
 * 
 */
public class ReplicationOperationsImpl implements ReplicationOperations {

  private Instance inst;
  private Credentials creds;

  public ReplicationOperationsImpl(Instance inst, Credentials creds) {
    this.inst = inst;
    this.creds = creds;
  }

  @Override
  public void addPeer(String name, ReplicaSystem system) throws AccumuloException, AccumuloSecurityException, PeerExistsException {
    checkNotNull(name);
    checkNotNull(system);

    addPeer(name, system.getClass().getName());
  }

  @Override
  public void addPeer(final String name, final String replicaType) throws AccumuloException, AccumuloSecurityException, PeerExistsException {
    checkNotNull(name);
    checkNotNull(replicaType);

    MasterClient.execute(inst, new ClientExec<Client>() {

      @Override
      public void execute(Client client) throws Exception {
        client.setSystemProperty(Tracer.traceInfo(), creds.toThrift(inst), Property.REPLICATION_PEERS.getKey() + name, replicaType);
      }

    });
  }

  @Override
  public void removePeer(final String name) throws AccumuloException, AccumuloSecurityException, PeerNotFoundException {
    checkNotNull(name);

    MasterClient.execute(inst, new ClientExec<Client>() {

      @Override
      public void execute(Client client) throws Exception {
        client.removeSystemProperty(Tracer.traceInfo(), creds.toThrift(inst), Property.REPLICATION_PEERS.getKey() + name);
      }

    });
  }
}
