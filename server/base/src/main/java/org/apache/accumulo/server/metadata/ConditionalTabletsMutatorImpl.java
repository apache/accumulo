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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ConditionalTabletsMutatorImpl implements Ample.ConditionalTabletsMutator {

  private static final Logger log = LoggerFactory.getLogger(ConditionalTabletsMutatorImpl.class);

  private final ServerContext context;
  private Ample.DataLevel dataLevel = null;

  private List<ConditionalMutation> mutations = new ArrayList<>();

  private Map<Text,KeyExtent> extents = new HashMap<>();

  private boolean active = true;

  Map<KeyExtent,Ample.RejectionHandler> rejectedHandlers = new HashMap<>();
  private final Function<DataLevel,String> tableMapper;

  public ConditionalTabletsMutatorImpl(ServerContext context) {
    this(context, DataLevel::metaTable);
  }

  public ConditionalTabletsMutatorImpl(ServerContext context,
      Function<DataLevel,String> tableMapper) {
    this.context = context;
    this.tableMapper = Objects.requireNonNull(tableMapper);
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
        "Duplicate extents not handled %s", extent);
    return new ConditionalTabletMutatorImpl(this, context, extent, mutations::add,
        rejectedHandlers::put);
  }

  protected ConditionalWriter createConditionalWriter(Ample.DataLevel dataLevel)
      throws TableNotFoundException {
    if (dataLevel == Ample.DataLevel.ROOT) {
      return new RootConditionalWriter(context);
    } else {
      return context.createConditionalWriter(getTableMapper().apply(dataLevel));
    }
  }

  protected Map<KeyExtent,TabletMetadata> readTablets(List<KeyExtent> extents) {
    Map<KeyExtent,TabletMetadata> failedTablets = new HashMap<>();

    try (var tabletsMeta = context.getAmple().readTablets().forTablets(extents, Optional.empty())
        .saveKeyValues().build()) {
      tabletsMeta
          .forEach(tabletMetadata -> failedTablets.put(tabletMetadata.getExtent(), tabletMetadata));
    }

    return failedTablets;
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

    return readTablets(extents);
  }

  private void partitionResults(Iterator<ConditionalWriter.Result> results,
      List<ConditionalWriter.Result> resultsList, List<ConditionalWriter.Result> unknownResults) {
    while (results.hasNext()) {
      var result = results.next();

      try {
        if (result.getStatus() == ConditionalWriter.Status.UNKNOWN) {
          unknownResults.add(result);
        } else {
          resultsList.add(result);
        }
      } catch (AccumuloException | AccumuloSecurityException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private Iterator<ConditionalWriter.Result> writeMutations(ConditionalWriter conditionalWriter) {
    var results = conditionalWriter.write(mutations.iterator());

    List<ConditionalWriter.Result> resultsList = new ArrayList<>();
    List<ConditionalWriter.Result> unknownResults = new ArrayList<>();
    partitionResults(results, resultsList, unknownResults);

    Retry retry = null;

    while (!unknownResults.isEmpty()) {
      try {
        if (retry == null) {
          retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(100))
              .incrementBy(Duration.ofMillis(100)).maxWait(Duration.ofSeconds(2)).backOffFactor(1.5)
              .logInterval(Duration.ofMinutes(3)).createRetry();
        }
        retry.waitForNextAttempt(log, "handle conditional mutations with unknown status");
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      results = conditionalWriter
          .write(unknownResults.stream().map(ConditionalWriter.Result::getMutation).iterator());

      // create a new array instead of clearing in case the above has not consumed everything
      unknownResults = new ArrayList<>();

      partitionResults(results, resultsList, unknownResults);
    }

    return resultsList.iterator();
  }

  private Ample.ConditionalResult.Status mapStatus(KeyExtent extent,
      ConditionalWriter.Result result) {

    ConditionalWriter.Status status = null;
    try {
      status = result.getStatus();
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IllegalStateException(e);
    }

    switch (status) {
      case REJECTED:
        return Ample.ConditionalResult.Status.REJECTED;
      case ACCEPTED:
        return Ample.ConditionalResult.Status.ACCEPTED;
      default:
        throw new IllegalStateException(
            "Unexpected conditional mutation status : " + extent + " " + status);
    }
  }

  @Override
  public Map<KeyExtent,Ample.ConditionalResult> process() {
    Preconditions.checkState(active);
    if (dataLevel != null) {
      try (ConditionalWriter conditionalWriter = createConditionalWriter(dataLevel)) {
        var results = writeMutations(conditionalWriter);

        var resultsMap = new HashMap<KeyExtent,ConditionalWriter.Result>();

        while (results.hasNext()) {
          var result = results.next();
          var row = new Text(result.getMutation().getRow());
          resultsMap.put(extents.get(row), result);
        }

        var extentsSet = Set.copyOf(extents.values());
        ensureAllExtentsSeen(resultsMap, extentsSet);

        // only fetch the metadata for failures when requested and when it is requested fetch all
        // of the failed extents at once to avoid fetching them one by one.
        var failedMetadata = Suppliers.memoize(() -> readFailedTablets(resultsMap));

        return Maps.transformEntries(resultsMap, (extent, result) -> new Ample.ConditionalResult() {

          private Ample.ConditionalResult.Status _getStatus() {
            return mapStatus(extent, result);
          }

          @Override
          public Status getStatus() {
            var status = _getStatus();
            if (status == Status.REJECTED && rejectedHandlers.containsKey(extent)) {
              var tabletMetadata = readMetadata();
              var handler = rejectedHandlers.get(extent);
              if (tabletMetadata == null && handler.callWhenTabletDoesNotExists()
                  && handler.test(null)) {
                return Status.ACCEPTED;
              }
              if (tabletMetadata != null && handler.test(tabletMetadata)) {
                return Status.ACCEPTED;
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
            Preconditions.checkState(_getStatus() != Status.ACCEPTED,
                "Can not read metadata for mutations with a status of " + Status.ACCEPTED);
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

  private void ensureAllExtentsSeen(HashMap<KeyExtent,ConditionalWriter.Result> resultsMap,
      Set<KeyExtent> extentsSet) {
    if (!resultsMap.keySet().equals(Set.copyOf(extents.values()))) {
      Sets.difference(resultsMap.keySet(), extentsSet)
          .forEach(extent -> log.error("Unexpected extent seen in in result {}", extent));

      Sets.difference(extentsSet, resultsMap.keySet())
          .forEach(extent -> log.error("Expected extent not seen in result {}", extent));

      resultsMap.forEach((keyExtent, result) -> {
        log.error("result seen {} {}", keyExtent, new Text(result.getMutation().getRow()));
      });

      // There are at least two possible causes for this condition. First there is a bug in the
      // ConditionalWriter, and it did not return a result for a mutation that was given to it.
      // Second, Ample code was not used correctly and mutating an extent was started but never
      // submitted.
      throw new IllegalStateException("Not all extents were seen, this is unexpected.");
    }
  }

  @Override
  public void close() {}

  protected Function<DataLevel,String> getTableMapper() {
    return tableMapper;
  }

}
