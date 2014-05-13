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
package org.apache.accumulo.core.client.admin;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.replication.PeerExistsException;
import org.apache.accumulo.core.client.replication.PeerNotFoundException;
import org.apache.accumulo.core.client.replication.ReplicaSystem;

/**
 * Supports replication configuration
 * @since 1.7.0
 */
public interface ReplicationOperations {

  /**
   * Define a cluster with the given name using the given {@link ReplicaSystem}
   * @param name Name of the cluster, used for configuring replication on tables
   * @param system Type of system to be replicated to
   */
  public void addPeer(String name, ReplicaSystem system) throws AccumuloException, AccumuloSecurityException, PeerExistsException;

  /**
   * Define a cluster with the given name and the given name system
   * @param name Unique name for the cluster
   * @param replicaType {@link ReplicaSystem} class name to use to replicate the data 
   * @throws PeerExistsException
   */
  public void addPeer(String name, String replicaType) throws AccumuloException, AccumuloSecurityException, PeerExistsException;

  /**
   * Remove a cluster with the given name
   * @param name Name of the cluster to remove
   * @throws PeerNotFoundException
   */
  public void removePeer(String name) throws AccumuloException, AccumuloSecurityException, PeerNotFoundException;

  /**
   * Wait for a table to be fully replicated
   * @param tableName The table to wait for
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   */
  public void drain(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
}
