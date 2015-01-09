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
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.hadoop.io.Text;

public class MetaDataStateStore extends TabletStateStore {
  // private static final Logger log = Logger.getLogger(MetaDataStateStore.class);

  private static final int THREADS = 4;
  private static final int LATENCY = 1000;
  private static final int MAX_MEMORY = 200 * 1024 * 1024;

  final protected Instance instance;
  final protected CurrentState state;
  final protected Credentials credentials;
  final private String targetTableName;

  protected MetaDataStateStore(Instance instance, Credentials credentials, CurrentState state, String targetTableName) {
    this.instance = instance;
    this.state = state;
    this.credentials = credentials;
    this.targetTableName = targetTableName;
  }

  public MetaDataStateStore(Instance instance, Credentials credentials, CurrentState state) {
    this(instance, credentials, state, MetadataTable.NAME);
  }

  protected MetaDataStateStore(String tableName) {
    this(HdfsZooInstance.getInstance(), SystemCredentials.get(), null, tableName);
  }

  public MetaDataStateStore() {
    this(MetadataTable.NAME);
  }

  @Override
  public ClosableIterator<TabletLocationState> iterator() {
    return new MetaDataTableScanner(instance, credentials, MetadataSchema.TabletsSection.getRange(), state);
  }

  @Override
  public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    BatchWriter writer = createBatchWriter();
    try {
      for (Assignment assignment : assignments) {
        Mutation m = new Mutation(assignment.tablet.getMetadataEntry());
        Text cq = assignment.server.asColumnQualifier();
        m.put(TabletsSection.CurrentLocationColumnFamily.NAME, cq, assignment.server.asMutationValue());
        m.putDelete(TabletsSection.FutureLocationColumnFamily.NAME, cq);
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
      return instance.getConnector(credentials.getPrincipal(), credentials.getToken()).createBatchWriter(targetTableName,
          new BatchWriterConfig().setMaxMemory(MAX_MEMORY).setMaxLatency(LATENCY, TimeUnit.MILLISECONDS).setMaxWriteThreads(THREADS));
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
        m.put(TabletsSection.FutureLocationColumnFamily.NAME, assignment.server.asColumnQualifier(), assignment.server.asMutationValue());
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
          m.putDelete(TabletsSection.CurrentLocationColumnFamily.NAME, tls.current.asColumnQualifier());
        }
        if (tls.future != null) {
          m.putDelete(TabletsSection.FutureLocationColumnFamily.NAME, tls.future.asColumnQualifier());
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
