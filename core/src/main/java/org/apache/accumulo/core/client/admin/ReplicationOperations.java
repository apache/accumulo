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

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.replication.PeerExistsException;
import org.apache.accumulo.core.client.replication.PeerNotFoundException;

/**
 * Supports replication configuration
 *
 * @since 1.7.0
 */
public interface ReplicationOperations {

  /**
   * Defines a cluster with the given name and the given name system.
   *
   * @param name
   *          Unique name for the cluster
   * @param replicaType
   *          Class name to use to replicate the data
   */
  public void addPeer(String name, String replicaType) throws AccumuloException, AccumuloSecurityException, PeerExistsException;

  /**
   * Removes a cluster with the given name.
   *
   * @param name
   *          Name of the cluster to remove
   */
  public void removePeer(String name) throws AccumuloException, AccumuloSecurityException, PeerNotFoundException;

  /**
   * Waits for a table to be fully replicated, given the state of files pending replication for the provided table at the point in time which this method is
   * invoked.
   *
   * @param tableName
   *          The table to wait for
   */
  public void drain(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * Given the provided set of files that are pending replication for a table, wait for those files to be fully replicated to all configured peers. This allows
   * for the accurate calculation when a table, at a given point in time, has been fully replicated.
   *
   * @param tableName
   *          The table to wait for
   */
  public void drain(String tableName, Set<String> files) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;

  /**
   * Gets all of the referenced files for a table from the metadata table. The result of this method is intended to be directly supplied to
   * {@link #drain(String, Set)}. This helps determine when all data from a given point in time has been fully replicated.
   * <p>
   * This also allows callers to get the {@link Set} of files for a table at some time, and later provide that {@link Set} to {@link #drain(String,Set)} to wait
   * for all of those files to be replicated.
   */
  public Set<String> referencedFiles(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
}
