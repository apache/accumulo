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
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * If the TabletMetadata contains the supplied key, then return the TabletMetadata
 */
public class HasColumnFilter extends TabletMetadataFilter {

  public static final String COLUMN_OPT = "HasColumnFilter_Column";
  public static final String INCLUDE_OPT = "HasColumnFilter_Include";

  private Map<String,String> options = null;

  private Predicate<TabletMetadata> pred;

  public HasColumnFilter() {}

  public HasColumnFilter(ColumnType type, boolean include) {
    Objects.requireNonNull(type, "Type must be supplied");
    options = Map.of(COLUMN_OPT, type.name(), INCLUDE_OPT, Boolean.toString(include));
  }

  public HasColumnFilter(Key key, boolean include) {
    Objects.requireNonNull(key, "Key must be supplied");
    ColumnType type = findMatchingColumn(key);
    Objects.requireNonNull(type, "Key did not match any known column");
    options = Map.of(COLUMN_OPT, type.name(), INCLUDE_OPT, Boolean.toString(include));
  }

  private ColumnType findMatchingColumn(Key key) {
    ColumnType result = null;
    for (Entry<ColumnType,ColumnFQ> e : ColumnType.COLUMNS_TO_QUALIFIERS.entrySet()) {
      if (e.getValue().hasColumns(key)) {
        result = e.getKey();
        break;
      }
    }
    if (result == null) {
      // Did not matching on colf:colq, check to see if colf matches
      for (Entry<ColumnType,Set<Text>> e : ColumnType.COLUMNS_TO_FAMILIES.entrySet()) {
        if (e.getValue().contains(key.getColumnFamily())) {
          result = e.getKey();
          break;
        }
      }
    }
    return result;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    String colOpt = options.get(COLUMN_OPT);
    String includeOpt = options.get(INCLUDE_OPT);
    Objects.requireNonNull(colOpt, "Setting COLUMN_OPT is required");
    Objects.requireNonNull(includeOpt, "Setting INCLUDE_OPT is required");
    ColumnType type = ColumnType.valueOf(colOpt);
    Boolean include = Boolean.parseBoolean(includeOpt);
    pred = (tm) -> {
      boolean columnFound = false;
      for (Entry<Key,Value> e : tm.getKeyValues()) {
        ColumnType ct = findMatchingColumn(e.getKey());
        if (ct != null && ct == type) {
          columnFound = true;
        }
      }
      return include && columnFound;
    };
  }

  @Override
  public Map<String,String> getServerSideOptions() {
    Preconditions.checkState(options != null);
    return options;
  }

  @Override
  public Set<ColumnType> getColumns() {
    return EnumSet.allOf(ColumnType.class);
  }

  @Override
  protected Predicate<TabletMetadata> acceptTablet() {
    return pred;
  }

}
