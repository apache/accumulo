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

import static org.apache.accumulo.server.util.MetadataTableUtil.EMPTY_TEXT;

import java.util.Collection;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.AmpleImpl;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class ServerAmpleImpl extends AmpleImpl implements Ample {

  private ServerContext context;

  public ServerAmpleImpl(ServerContext ctx) {
    super(ctx);
    this.context = ctx;
  }

  @Override
  public Ample.TabletMutator mutateTablet(KeyExtent extent) {
    TabletsMutator tmi = mutateTablets();
    Ample.TabletMutator tabletMutator = tmi.mutateTablet(extent);
    ((TabletMutatorBase) tabletMutator).setCloseAfterMutate(tmi);
    return tabletMutator;
  }

  @Override
  public TabletsMutator mutateTablets() {
    return new TabletsMutatorImpl(context);
  }

  @Override
  public void putGcCandidates(TableId tableId, Collection<? extends Ample.FileMeta> candidates) {
    try (BatchWriter writer = createWriter(tableId)) {
      for (Ample.FileMeta file : candidates) {
        writer.addMutation(createDeleteMutation(context, tableId, file.path().toString()));
      }
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteGcCandidates(TableId tableId, Collection<String> paths) {
    try (BatchWriter writer = createWriter(tableId)) {
      for (String path : paths) {
        Mutation m = new Mutation(MetadataSchema.DeletesSection.getRowPrefix() + path);
        m.putDelete(EMPTY_TEXT, EMPTY_TEXT);
        writer.addMutation(m);
      }
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  private BatchWriter createWriter(TableId tableId) {

    Preconditions.checkArgument(!RootTable.ID.equals(tableId));

    try {
      if (MetadataTable.ID.equals(tableId)) {
        return context.createBatchWriter(RootTable.NAME);
      } else {
        return context.createBatchWriter(MetadataTable.NAME);
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static Mutation createDeleteMutation(ServerContext context, TableId tableId,
      String pathToRemove) {
    Path path = context.getVolumeManager().getFullPath(tableId, pathToRemove);
    Mutation delFlag = new Mutation(new Text(MetadataSchema.DeletesSection.getRowPrefix() + path));
    delFlag.put(EMPTY_TEXT, EMPTY_TEXT, new Value(new byte[] {}));
    return delFlag;
  }

}
