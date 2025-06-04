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
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.collect.Iterators;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorAdapter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;

import com.google.common.base.Preconditions;

public abstract class TabletMetadataFilter extends WrappingIterator {
  private Iterator<TabletMetadata> filteredTablets;
  private SortedKeyValueIterator<Key,Value> seachIterator;
  private Collection<ByteSequence> seekFamilies;
  private boolean seekInclusive;
  private boolean hasTop = false;
  private Map<String,String> options;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    this.seachIterator = source.deepCopy(env);
    this.options = Map.copyOf(options);
  }

  @Override
  public boolean hasTop() {
    return hasTop;
  }

  @Override
  public void next() throws IOException {
    super.next();
    if (!super.hasTop()) {
      // the main iterator was exhausted for the current row its on, see if the search iterator has
      // anything.
      seekToNextTablet();
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    this.seekFamilies = columnFamilies;
    this.seekInclusive = inclusive;

    // Setup a search iterator that only uses the column families needed for filtering and not all
    // the columns passed to this function. This optimizes a case like where we want only tablets
    // that have external compactions, but when we find a tablet we want the ecomp and prev row
    // columns. So when searching for tablets only seek on the ecomp column and avoid the seek with
    // the prev row column until a tablet is found.
    var resolvedFamilies = TabletMetadata.ColumnType.resolveFamilies(getColumns());
    Preconditions.checkState(!resolvedFamilies.isEmpty());
    seachIterator.seek(range, resolvedFamilies, true);
    var rowsIter = new RowIterator(new IteratorAdapter(seachIterator));
    var cols = EnumSet.copyOf(getColumns());
    var tabletsIter = Iterators.transform(rowsIter, rowIter -> TabletMetadata.convertRow(rowIter, cols, true, false));
    var predicate = acceptTablet();
    filteredTablets = Iterators.filter(tabletsIter, predicate::test);
    seekToNextTablet();
  }

  private void seekToNextTablet() throws IOException {
    if (filteredTablets.hasNext()) {
      var nextTablet = filteredTablets.next();
      var row = nextTablet.getKeyValues().get(0).getKey().getRow();
      // now that a row was found by our serach iterator, seek the main iterator with all the
      // columns for that row range
      super.seek(new Range(row), seekFamilies, seekInclusive);
      while (!super.hasTop() && filteredTablets.hasNext()) {
        nextTablet = filteredTablets.next();
        row = nextTablet.getKeyValues().get(0).getKey().getRow();
        super.seek(new Range(row), seekFamilies, seekInclusive);
      }
      hasTop = super.hasTop();
    } else {
      hasTop = false;
    }
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    TabletMetadataFilter newInstance;
    try {
      newInstance = getClass().getDeclaredConstructor().newInstance();
      newInstance.init(getSource().deepCopy(env), options, env);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    newInstance.seachIterator = getSource().deepCopy(env);
    newInstance.options = options;
    return newInstance;
  }

  public abstract Set<TabletMetadata.ColumnType> getColumns();

  protected abstract Predicate<TabletMetadata> acceptTablet();

  public Map<String,String> getServerSideOptions() {
    return Map.of();
  }
}
