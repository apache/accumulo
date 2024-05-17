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

import java.util.Objects;
import java.util.function.Function;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.TabletsMutator;
import org.apache.accumulo.server.ServerContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class TabletsMutatorImpl implements TabletsMutator {

  private final ServerContext context;

  private BatchWriter rootWriter;
  private BatchWriter metaWriter;
  private final Function<DataLevel,String> tableMapper;

  @VisibleForTesting
  public TabletsMutatorImpl(ServerContext context, Function<DataLevel,String> tableMapper) {
    this.context = context;
    this.tableMapper = Objects.requireNonNull(tableMapper);
  }

  private BatchWriter getWriter(TableId tableId) {

    Preconditions.checkArgument(!AccumuloTable.ROOT.tableId().equals(tableId));

    try {
      if (AccumuloTable.METADATA.tableId().equals(tableId)) {
        if (rootWriter == null) {
          rootWriter = context.createBatchWriter(tableMapper.apply(DataLevel.METADATA));
        }

        return rootWriter;
      } else {
        if (metaWriter == null) {
          metaWriter = context.createBatchWriter(tableMapper.apply(DataLevel.USER));
        }

        return metaWriter;
      }
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(e);
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
      throw new IllegalStateException(e);
    }

  }
}
