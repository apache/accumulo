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
package org.apache.accumulo.server.master.state;

import java.util.Collection;
import java.util.Iterator;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.hadoop.io.Text;

public class MetaDataStateStore extends TabletStateStore {
  // private static final Logger log = Logger.getLogger(MetaDataStateStore.class);
  
  private static final int THREADS = 4;
  private static final int LATENCY = 1000;
  private static final int MAX_MEMORY = 200 * 1024 * 1024;
  
  final protected Instance instance;
  final protected CurrentState state;
  final protected AuthInfo auths;
  
  public MetaDataStateStore(Instance instance, AuthInfo auths, CurrentState state) {
    this.instance = instance;
    this.state = state;
    this.auths = auths;
  }
  
  public MetaDataStateStore() {
    this(HdfsZooInstance.getInstance(), SecurityConstants.getSystemCredentials(), null);
  }

  @Override
  public Iterator<TabletLocationState> iterator() {
    return new MetaDataTableScanner(instance, auths, Constants.NON_ROOT_METADATA_KEYSPACE, state);
  }
  
  @Override
  public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    BatchWriter writer = createBatchWriter();
    try {
      for (Assignment assignment : assignments) {
        Mutation m = new Mutation(assignment.tablet.getMetadataEntry());
        Text cq = assignment.server.asColumnQualifier();
        m.put(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY, cq, assignment.server.asMutationValue());
        m.putDelete(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY, cq);
        writer.addMutation(m);
      }
    } catch (Exception ex) {
      throw new DistributedStoreException(ex);
    } finally {
      try {
        writer.close();
      } catch (MutationsRejectedException e) {
        throw new DistributedStoreException(e);
      }
    }
  }
  
  BatchWriter createBatchWriter() {
    try {
      return instance.getConnector(auths).createBatchWriter(Constants.METADATA_TABLE_NAME, MAX_MEMORY, LATENCY, THREADS);
    } catch (TableNotFoundException e) {
      // ya, I don't think so
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void setFutureLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    BatchWriter writer = createBatchWriter();
    try {
      for (Assignment assignment : assignments) {
        Mutation m = new Mutation(assignment.tablet.getMetadataEntry());
        m.put(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY, assignment.server.asColumnQualifier(), assignment.server.asMutationValue());
        writer.addMutation(m);
      }
    } catch (Exception ex) {
      throw new DistributedStoreException(ex);
    } finally {
      try {
        writer.close();
      } catch (MutationsRejectedException e) {
        throw new DistributedStoreException(e);
      }
    }
  }
  
  @Override
  public void unassign(Collection<TabletLocationState> tablets) throws DistributedStoreException {
    
    BatchWriter writer = createBatchWriter();
    try {
      for (TabletLocationState tls : tablets) {
        Mutation m = new Mutation(tls.extent.getMetadataEntry());
        if (tls.current != null) {
          m.putDelete(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY, tls.current.asColumnQualifier());
        }
        if (tls.future != null) {
          m.putDelete(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY, tls.future.asColumnQualifier());
        }
        writer.addMutation(m);
      }
    } catch (Exception ex) {
      throw new DistributedStoreException(ex);
    } finally {
      try {
        writer.close();
      } catch (MutationsRejectedException e) {
        throw new DistributedStoreException(e);
      }
    }
  }
  
  @Override
  public String name() {
    return "Normal Tablets";
  }
}
