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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;

public class ConditionalTabletsMutatorImpl implements Ample.ConditionalTabletsMutator {

  private final ServerContext context;
  private Ample.DataLevel dataLevel = null;

  private List<ConditionalMutation> mutations = new ArrayList<>();

  private Map<Text,KeyExtent> extents = new HashMap<>();

  private boolean active = true;

  Map<KeyExtent,Ample.UknownValidator> unknownValidators = new HashMap<>();

  public ConditionalTabletsMutatorImpl(ServerContext context) {
    this.context = context;
  }

  @Override
  public Ample.OperationRequirements mutateTablet(KeyExtent extent) {
    Preconditions.checkState(active);

    var dataLevel = Ample.DataLevel.of(extent.tableId());

    if (this.dataLevel == null) {
      this.dataLevel = dataLevel;
    } else if (!this.dataLevel.equals(dataLevel)) {
      throw new IllegalArgumentException(
          "Can not mix data levels " + this.dataLevel + " " + dataLevel);
    }

    Preconditions.checkState(extents.putIfAbsent(extent.toMetaRow(), extent) == null,
        "Duplicate extents not handled");
    return new ConditionalTabletMutatorImpl(this, context, extent, mutations::add,
        unknownValidators::put);
  }

  private ConditionalWriter createConditionalWriter(Ample.DataLevel dataLevel)
      throws TableNotFoundException {
    if (dataLevel == Ample.DataLevel.ROOT) {
      return new RootConditionalWriter(context);
    } else {
      return context.createConditionalWriter(dataLevel.metaTable());
    }
  }

  private Map<KeyExtent,TabletMetadata>
      readFailedTablets(Map<KeyExtent,ConditionalWriter.Result> results) {

    var extents = results.entrySet().stream().filter(e -> {
      try {
        return e.getValue().getStatus() != ConditionalWriter.Status.ACCEPTED;
      } catch (AccumuloException | AccumuloSecurityException ex) {
        throw new RuntimeException(ex);
      }
    }).map(Map.Entry::getKey).collect(Collectors.toList());

    if (extents.isEmpty()) {
      return Map.of();
    }

    Map<KeyExtent,TabletMetadata> failedTablets = new HashMap<>();

    try (var tabletsMeta = context.getAmple().readTablets().forTablets(extents).build()) {
      tabletsMeta
          .forEach(tabletMetadata -> failedTablets.put(tabletMetadata.getExtent(), tabletMetadata));
    }

    return failedTablets;
  }

  @Override
  public Map<KeyExtent,Ample.ConditionalResult> process() {
    Preconditions.checkState(active);
    if (dataLevel != null) {
      try (ConditionalWriter conditionalWriter = createConditionalWriter(dataLevel)) {
        var results = conditionalWriter.write(mutations.iterator());

        var resultsMap = new HashMap<KeyExtent,ConditionalWriter.Result>();

        while (results.hasNext()) {
          var result = results.next();
          var row = new Text(result.getMutation().getRow());
          resultsMap.put(extents.get(row), result);
        }

        if (!resultsMap.keySet().equals(Set.copyOf(extents.values()))) {
          throw new AssertionError("Not all extents were seen, this is unexpected");
        }

        // only fetch the metadata for failures when requested and when it is requested fetch all
        // of the failed extents at once to avoid fetching them one by one.
        var failedMetadata = Suppliers.memoize(() -> readFailedTablets(resultsMap));

        return Maps.transformEntries(resultsMap, (extent, result) -> new Ample.ConditionalResult() {

          private ConditionalWriter.Status _getStatus() {
            try {
              return result.getStatus();
            } catch (AccumuloException | AccumuloSecurityException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public ConditionalWriter.Status getStatus() {
            var status = _getStatus();
            if (status == ConditionalWriter.Status.UNKNOWN
                && unknownValidators.containsKey(extent)) {
              var tabletMetadata = readMetadata();
              if (tabletMetadata != null
                  && unknownValidators.get(extent).shouldAccept(tabletMetadata)) {
                return ConditionalWriter.Status.ACCEPTED;
              }
            }

            return status;
          }

          @Override
          public KeyExtent getExtent() {
            return extent;
          }

          @Override
          public TabletMetadata readMetadata() {
            Preconditions.checkState(_getStatus() != ConditionalWriter.Status.ACCEPTED);
            return failedMetadata.get().get(getExtent());
          }
        });
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
  public void close() {}
}
