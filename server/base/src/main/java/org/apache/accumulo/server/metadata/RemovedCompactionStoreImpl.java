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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.RemovedCompactionSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;

import com.google.common.base.Preconditions;

public class RemovedCompactionStoreImpl implements Ample.RemovedCompactionStore {
  private final ServerContext context;

  public RemovedCompactionStoreImpl(ServerContext context) {
    this.context = context;
  }

  private Stream<Ample.RemovedCompaction> createStream(String tableName) {
    Scanner scanner = null;
    try {
      scanner = context.createScanner(tableName, Authorizations.EMPTY);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(e);
    }
    scanner.setRange(RemovedCompactionSection.getRange());
    return scanner.stream().map(e -> e.getKey().getRowData().toString())
        .map(RemovedCompactionSection::decodeRow).onClose(scanner::close);
  }

  @Override
  public Stream<Ample.RemovedCompaction> list() {
    return Stream.concat(createStream(Ample.DataLevel.METADATA.metaTable()),
        createStream(Ample.DataLevel.USER.metaTable()));
  }

  private void write(Collection<Ample.RemovedCompaction> removedCompactions,
      Function<Ample.RemovedCompaction,Mutation> converter) {
    if (removedCompactions.isEmpty()) {
      return;
    }

    Map<Ample.DataLevel,List<Ample.RemovedCompaction>> byLevel = removedCompactions.stream()
        .collect(Collectors.groupingBy(rc -> Ample.DataLevel.of(rc.table())));
    // Do not expect the root to split or merge so it should never have this data
    Preconditions.checkArgument(!byLevel.containsKey(Ample.DataLevel.ROOT));
    byLevel.forEach((dl, removed) -> {
      try (var writer = context.createBatchWriter(dl.metaTable())) {
        for (var rc : removed) {
          writer.addMutation(converter.apply(rc));
        }
      } catch (TableNotFoundException | MutationsRejectedException e) {
        throw new IllegalStateException(e);
      }
    });
  }

  @Override
  public void add(Collection<Ample.RemovedCompaction> removedCompactions) {
    write(removedCompactions, rc -> {
      Mutation m = new Mutation(RemovedCompactionSection.encodeRow(rc));
      m.put("", "", "");
      return m;
    });

  }

  @Override
  public void delete(Collection<Ample.RemovedCompaction> removedCompactions) {
    write(removedCompactions, rc -> {
      Mutation m = new Mutation(RemovedCompactionSection.encodeRow(rc));
      m.putDelete("", "");
      return m;
    });
  }
}
