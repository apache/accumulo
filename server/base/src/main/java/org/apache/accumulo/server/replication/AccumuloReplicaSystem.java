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
package org.apache.accumulo.server.replication;

import java.nio.ByteBuffer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.ReplicationClient;
import org.apache.accumulo.core.client.impl.ServerConfigurationUtil;
import org.apache.accumulo.core.client.replication.ReplicaSystem;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer.Client;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 
 */
public class AccumuloReplicaSystem implements ReplicaSystem {
  private static final Logger log = LoggerFactory.getLogger(AccumuloReplicaSystem.class);

  private String instanceName, zookeepers;

  @Override
  public void configure(String configuration) {
    Preconditions.checkNotNull(configuration);

    int index = configuration.indexOf(',');
    if (-1 == index) {
      throw new IllegalArgumentException("Expected comma in configuration string");
    }

    instanceName = configuration.substring(0, index);
    zookeepers = configuration.substring(index + 1);
  }

  @Override
  public Status replicate(Path p, Status status, ReplicationTarget target) {
    Instance localInstance = HdfsZooInstance.getInstance();
    AccumuloConfiguration localConf = ServerConfigurationUtil.getConfiguration(localInstance);
    
    Instance peerInstance = getPeerInstance(target);
    // Remote identifier is an integer (table id) in this case.
    final int remoteTableId = Integer.parseInt(target.getRemoteIdentifier());

    // Attempt the replication of this status a number of times before giving up and
    // trying to replicate it again later some other time.
    for (int i = 0; i < localConf.getCount(Property.REPLICATION_WORK_ATTEMPTS); i++) {
      String peerTserver;
      try {
        // Ask the master on the remote what TServer we should talk with to replicate the data
        peerTserver = ReplicationClient.executeCoordinatorWithReturn(peerInstance, new ClientExecReturn<String,ReplicationCoordinator.Client>() {
  
          @Override
          public String execute(ReplicationCoordinator.Client client) throws Exception {
            return client.getServicerAddress(remoteTableId);
          }
          
        });
      } catch (AccumuloException | AccumuloSecurityException e) {
        // No progress is made
        log.error("Could not connect to master at {}, cannot proceed with replication. Will retry", target, e);
        continue;
      }
  
      if (null == peerTserver) {
        // Something went wrong, and we didn't get a valid tserver from the remote for some reason
        log.warn("Did not receive tserver from master at {}, cannot proceed with replication. Will retry.", target);
        continue;
      }
  
      // We have a tserver on the remote -- send the data its way.
      ByteBuffer result;
      //TODO should chunk up the given file into some configurable sizes instead of just sending the entire file all at once
      //     configuration should probably just be size based.
      try {
        result = ReplicationClient.executeServicerWithReturn(peerInstance, peerTserver, new ClientExecReturn<ByteBuffer,ReplicationServicer.Client>() {
          @Override
          public ByteBuffer execute(Client client) throws Exception {
            //TODO This needs to actually send the appropriate data, and choose replicateLog or replicateKeyValues
            return client.replicateLog(remoteTableId, null);
          }
        });

        // We need to be able to parse the returned Status,
        // if we can't, we don't know what the server actually parsed.
        try {
          return Status.parseFrom(ByteBufferUtil.toBytes(result));
        } catch (InvalidProtocolBufferException e) {
          log.error("Could not parse return Status from {}", peerTserver, e);
          throw new RuntimeException("Could not parse returned Status from " + peerTserver, e);
        }
      } catch (TTransportException | AccumuloException | AccumuloSecurityException e) {
        log.warn("Could not connect to remote server {}, will retry", peerTserver, e);
        UtilWaitThread.sleep(250);
      }
    }

    // We made no status, punt on it for now, and let it re-queue itself for work
    return status;
  }

  public Instance getPeerInstance(ReplicationTarget target) {
    return new ZooKeeperInstance(instanceName, zookeepers);
  }
}
