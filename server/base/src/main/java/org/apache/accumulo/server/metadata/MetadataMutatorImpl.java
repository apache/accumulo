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

package org.apache.accumulo.server.metadata;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.server.ServerContext;

import com.google.common.base.Preconditions;

public class MetadataMutatorImpl implements MetadataMutator {

  private ServerContext context;

  private BatchWriter rootWriter;
  private BatchWriter metaWriter;

  public MetadataMutatorImpl(ServerContext context) {
    this.context = context;
  }

  private BatchWriter getWriter(TableId tableId) {


    Preconditions.checkArgument(!RootTable.ID.equals(tableId));

    try {
    if(MetadataTable.ID.equals(tableId)) {
      if(rootWriter == null) {
        rootWriter = context.createBatchWriter(RootTable.NAME);
      }

      return rootWriter;
    } else {
      if(metaWriter == null) {
        metaWriter = context.createBatchWriter(MetadataTable.NAME);
      }

      return metaWriter;
    }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TabletMutator mutateTablet(KeyExtent extent) {
    if (extent.isRootTablet()) {
      return new RootTabletMutatorImpl(context);
    } else {
      return new TabletMutatorImpl(context, extent, getWriter(extent.getTableId()));
    }
  }

  @Override
  public GcMutator mutateDeletes(TableId tableId) {
    // TODO handle root tablet
    Preconditions.checkArgument(!RootTable.ID.equals(tableId));
    return new GcMutatorImpl(context, tableId, getWriter(tableId));
  }



  @Override
  public void flush() {
    try {
      if(rootWriter != null) {
        rootWriter.flush();
      }

      if(metaWriter != null) {
        metaWriter.flush();
      }
      } catch (MutationsRejectedException e) {
        throw new RuntimeException(e);
      }
  }

  @Override
  public void close() {
    try {
      if(rootWriter != null) {
        rootWriter.close();
      }

      if(metaWriter != null) {
        metaWriter.close();
      }
      } catch (MutationsRejectedException e) {
        throw new RuntimeException(e);
      }

  }
}
