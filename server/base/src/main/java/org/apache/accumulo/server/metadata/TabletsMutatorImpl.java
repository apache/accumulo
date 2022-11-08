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
package org.apache.accumulo.server.metadata;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.TabletsMutator;
import org.apache.accumulo.server.ServerContext;

import com.google.common.base.Preconditions;

public class TabletsMutatorImpl implements TabletsMutator {

  private ServerContext context;

  private BatchWriter rootWriter;
  private BatchWriter metaWriter;

  public TabletsMutatorImpl(ServerContext context) {
    this.context = context;
  }

  private BatchWriter getWriter(TableId tableId) {

    Preconditions.checkArgument(!RootTable.ID.equals(tableId));

    try {
      if (MetadataTable.ID.equals(tableId)) {
        if (rootWriter == null) {
          rootWriter = context.createBatchWriter(RootTable.NAME);
        }

        return rootWriter;
      } else {
        if (metaWriter == null) {
          metaWriter = context.createBatchWriter(MetadataTable.NAME);
        }

        return metaWriter;
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Ample.TabletMutator mutateTablet(KeyExtent extent) {
    if (extent.isRootTablet()) {
      return new RootTabletMutatorImpl(context);
    } else {
      return new TabletMutatorImpl(context, extent, getWriter(extent.tableId()));
    }
  }

  @Override
  public void close() {
    try {
      if (rootWriter != null) {
        rootWriter.close();
      }

      if (metaWriter != null) {
        metaWriter.close();
      }
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }

  }
}
