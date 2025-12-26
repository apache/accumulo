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
package org.apache.accumulo.core.metadata.schema.filters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * A filter used by the Accumulo GC to find tablets that either have walogs or are assigned to a
 * dead tablet server.
 */
public class GcWalsFilter extends TabletMetadataFilter {

  private Map<String,String> options = null;

  private Predicate<TabletMetadata> filter = null;

  private static final String LIVE_TSERVER_OPT = "liveTservers";

  public GcWalsFilter() {}

  public GcWalsFilter(Set<TServerInstance> liveTservers) {
    String lts = liveTservers.stream().map(TServerInstance::toString).peek(tsi -> {
      if (tsi.contains(",")) {
        throw new IllegalArgumentException(tsi);
      }
    }).collect(Collectors.joining(","));
    this.options = Map.of(LIVE_TSERVER_OPT, lts);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    var encodedLiveTservers = options.get(LIVE_TSERVER_OPT);
    Set<TServerInstance> liveTservers;
    if (encodedLiveTservers.isBlank()) {
      liveTservers = Set.of();
    } else {
      liveTservers = Arrays.stream(options.get(LIVE_TSERVER_OPT).split(","))
          .map(TServerInstance::new).collect(Collectors.toUnmodifiableSet());
    }

    filter = tm -> !tm.getLogs().isEmpty()
        || TabletState.compute(tm, liveTservers) == TabletState.ASSIGNED_TO_DEAD_SERVER;
  }

  private static final Set<ColumnType> COLUMNS =
      Sets.immutableEnumSet(ColumnType.LOGS, ColumnType.LOCATION, ColumnType.SUSPEND);

  @Override
  public Set<ColumnType> getColumns() {
    return COLUMNS;
  }

  @Override
  protected Predicate<TabletMetadata> acceptTablet() {
    return filter;
  }

  @Override
  public Map<String,String> getServerSideOptions() {
    Preconditions.checkState(options != null);
    return options;
  }
}
