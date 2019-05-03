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

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.metadata.MetadataMutator.GcMutator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class GcMutatorImpl implements GcMutator {

  private ServerContext context;
  private TableId tableId;
  private BatchWriter writer;

  public GcMutatorImpl(ServerContext context, TableId tableId, BatchWriter writer) {
    this.context = context;
    this.tableId = tableId;
    this.writer = writer;
  }

  @Override
  public void put(FileRef path) {
    try {
      writer.addMutation(createDeleteMutation(context, tableId, path.path().toString()));
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(String path) {
      Mutation m = new Mutation(MetadataSchema.DeletesSection.getRowPrefix() + path);
      m.putDelete(EMPTY_TEXT, EMPTY_TEXT);
      try {
        writer.addMutation(m);
      } catch (MutationsRejectedException e) {
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
