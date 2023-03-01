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
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.hadoop.io.Text;

public class ConditionalTabletsMutatorImpl implements Ample.ConditionalTabletsMutator {

  private final ClientContext context;
  private TableId currentTableId = null;

  private List<ConditionalMutation> mutations = new ArrayList<>();

  private Map<Text,KeyExtent> extents = new HashMap<>();

  public ConditionalTabletsMutatorImpl(ClientContext context) {
    this.context = context;
  }

  @Override
  public Ample.ConditionalTabletMutator mutateTablet(KeyExtent extent) {
    if (currentTableId == null) {
      currentTableId = extent.tableId();
    } else if (!currentTableId.equals(extent.tableId())) {
      throw new IllegalArgumentException(
          "Can not mix tables ids " + currentTableId + " " + extent.tableId());
    }

    // TODO handle case where it exists
    extents.put(extent.toMetaRow(), extent);

    return new ConditionalTabletMutatorImpl(context, extent, mutations::add);
  }

  @Override
  public Map<KeyExtent,ConditionalWriter.Result> process() {
    if (currentTableId != null) {
      var dataLevel = Ample.DataLevel.of(currentTableId);
      try (ConditionalWriter conditionalWriter =
          context.createConditionalWriter(dataLevel.metaTable())) {
        // TODO translate back to include key extent
        var results = conditionalWriter.write(mutations.iterator());

        var resultsMap = new HashMap<KeyExtent,ConditionalWriter.Result>();

        while (results.hasNext()) {
          var result = results.next();
          var row = new Text(result.getMutation().getRow());
          // TODO check if null
          resultsMap.put(extents.get(row), result);

        }

        return resultsMap;
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }

      // TODO clear state or make unusable

    } else {
      return Map.of();
    }
  }

  @Override
  public void close() {
    // TODO drop?
  }
}
