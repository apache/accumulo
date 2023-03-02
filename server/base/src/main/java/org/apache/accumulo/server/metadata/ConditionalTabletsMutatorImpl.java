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

import java.util.*;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class ConditionalTabletsMutatorImpl implements Ample.ConditionalTabletsMutator {

  private final ServerContext context;
  private TableId currentTableId = null;

  private List<ConditionalMutation> mutations = new ArrayList<>();

  private Map<Text,KeyExtent> extents = new HashMap<>();

  private boolean active = true;

  public ConditionalTabletsMutatorImpl(ServerContext context) {
    this.context = context;
  }

  @Override
  public Ample.ConditionalTabletMutator mutateTablet(KeyExtent extent) {
    Preconditions.checkState(active);
    if (currentTableId == null) {
      currentTableId = extent.tableId();
    } else if (!currentTableId.equals(extent.tableId())) {
      throw new IllegalArgumentException(
          "Can not mix tables ids " + currentTableId + " " + extent.tableId());
    }

    Preconditions.checkState(extents.putIfAbsent(extent.toMetaRow(), extent) == null,
        "Duplicate extents not handled");
    return new ConditionalTabletMutatorImpl(context, extent, mutations::add);
  }

  private ConditionalWriter createConditionalWriter(Ample.DataLevel dataLevel)
      throws TableNotFoundException {
    if (dataLevel == Ample.DataLevel.ROOT) {
      return new RootConditionalWriter(context);
    } else {
      return context.createConditionalWriter(dataLevel.metaTable());
    }
  }

  @Override
  public Map<KeyExtent,ConditionalWriter.Result> process() {
    Preconditions.checkState(active);
    if (currentTableId != null) {
      var dataLevel = Ample.DataLevel.of(currentTableId);
      try (ConditionalWriter conditionalWriter = createConditionalWriter(dataLevel)) {
        var results = conditionalWriter.write(mutations.iterator());

        var resultsMap = new HashMap<KeyExtent,ConditionalWriter.Result>();

        while (results.hasNext()) {
          var result = results.next();
          var row = new Text(result.getMutation().getRow());
          resultsMap.put(extents.get(row), result);
        }

        // TODO maybe this check is expensive
        if (!resultsMap.keySet().equals(Set.copyOf(extents.values()))) {
          throw new AssertionError("Not all extents were seen, this is unexpected");
        }

        return resultsMap;
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      } finally {
        // render inoperable because reuse is not tested
        extents.clear();
        mutations.clear();
        active = false;
      }
    } else {
      // render inoperable because reuse is not tested
      extents.clear();
      mutations.clear();
      active = false;
      return Map.of();
    }
  }

  @Override
  public void close() {
    // TODO drop?
  }
}
