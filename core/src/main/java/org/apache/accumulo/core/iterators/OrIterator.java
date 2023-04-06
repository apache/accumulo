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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An iterator that provides a sorted-iteration of column qualifiers for a set of column families in
 * a row. It is important to note that this iterator <em>does not</em> adhere to the contract set
 * forth by the {@link SortedKeyValueIterator}. It returns Keys in {@code row+colqual} order instead
 * of {@code row+colfam+colqual} order. This is required for the implementation of this iterator (to
 * work in conjunction with the {@code IntersectingIterator}) but is a code-smell. This iterator
 * should only be used at query time, never at compaction time.
 *
 * The table structure should have the following form:
 *
 * <pre>
 * row term:docId =&gt; value
 * </pre>
 *
 * Users configuring this iterator must set the option {@link #COLUMNS_KEY}. This value is a
 * comma-separated list of column families that should be "OR"'ed together.
 *
 * For example, given the following data and a value of {@code or.iterator.columns="steve,bob"} in
 * the iterator options map:
 *
 * <pre>
 * row1 bob:4
 * row1 george:2
 * row1 steve:3
 * row2 bob:9
 * row2 frank:8
 * row2 steve:12
 * row3 michael:15
 * row3 steve:20
 * </pre>
 *
 * Would return:
 *
 * <pre>
 * row1 steve:3
 * row1 bob:4
 * row2 bob:9
 * row2 steve:12
 * row3 steve:20
 * </pre>
 */
public class OrIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
  private static final Logger LOG = LoggerFactory.getLogger(OrIterator.class);
  public static final String COLUMNS_KEY = "or.iterator.columns";

  private TermSource currentTerm;
  private List<TermSource> sources;
  private PriorityQueue<TermSource> sorted = new PriorityQueue<>(5);

  protected static class TermSource implements Comparable<TermSource> {
    private final SortedKeyValueIterator<Key,Value> iter;
    private final Text term;
    private final Collection<ByteSequence> seekColfams;
    private Range currentRange;

    public TermSource(TermSource other) {
      this.iter = Objects.requireNonNull(other.iter);
      this.term = Objects.requireNonNull(other.term);
      this.seekColfams = Objects.requireNonNull(other.seekColfams);
      this.currentRange = Objects.requireNonNull(other.currentRange);
    }

    public TermSource(SortedKeyValueIterator<Key,Value> iter, Text term) {
      this.iter = Objects.requireNonNull(iter);
      this.term = Objects.requireNonNull(term);
      // The desired column families for this source is the term itself
      this.seekColfams =
          Collections.singletonList(new ArrayByteSequence(term.getBytes(), 0, term.getLength()));
      // No current range until we're seek()'ed for the first time
      this.currentRange = null;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(iter.getTopKey().getColumnQualifier());
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this || (obj instanceof TermSource && compareTo((TermSource) obj) == 0);
    }

    @Override
    public int compareTo(TermSource o) {
      // NOTE: If your implementation can have more than one row in a tablet,
      // you must compare row key here first, then column qualifier.
      // NOTE2: A null check is not needed because things are only added to the
      // sorted after they have been determined to be valid.
      return this.iter.getTopKey().compareColumnQualifier(o.iter.getTopKey().getColumnQualifier());
    }

    /**
     * Converts the given {@code Range} into the correct {@code Range} for this TermSource (per this
     * expected table structure) and then seeks this TermSource's SKVI.
     */
    public void seek(Range originalRange) throws IOException {
      // the infinite start key is equivalent to a null startKey on the Range.
      if (originalRange.isInfiniteStartKey()) {
        currentRange = originalRange;
      } else {
        Key originalStartKey = originalRange.getStartKey();
        // Pivot the provided range into the range for this term
        Key newKey = new Key(originalStartKey.getRow(), term, originalStartKey.getColumnQualifier(),
            originalStartKey.getTimestamp());
        // Construct the new range, preserving the other attributes on the provided range.
        currentRange = new Range(newKey, originalRange.isStartKeyInclusive(),
            originalRange.getEndKey(), originalRange.isEndKeyInclusive());
      }
      LOG.trace("Seeking {} to {}", this, currentRange);
      iter.seek(currentRange, seekColfams, true);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("TermSource{term=").append(term).append(", currentRange=").append(currentRange)
          .append("}");
      return sb.toString();
    }

    /**
     * @return True if there is a valid topKey which falls into the range this TermSource's iterator
     *         was last seeked to, false otherwise.
     */
    boolean hasEntryForTerm() {
      if (!iter.hasTop()) {
        return false;
      }
      return currentRange.contains(iter.getTopKey());
    }
  }

  public OrIterator() {
    this.sources = Collections.emptyList();
  }

  private OrIterator(OrIterator other, IteratorEnvironment env) {
    ArrayList<TermSource> copiedSources = new ArrayList<>();

    for (TermSource TS : other.sources) {
      copiedSources.add(new TermSource(TS.iter.deepCopy(env), new Text(TS.term)));
    }
    this.sources = Collections.unmodifiableList(copiedSources);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new OrIterator(this, env);
  }

  public void setTerms(SortedKeyValueIterator<Key,Value> source, Collection<String> terms,
      IteratorEnvironment env) {
    ArrayList<TermSource> newTerms = new ArrayList<>();
    for (String term : terms) {
      newTerms.add(new TermSource(source.deepCopy(env), new Text(term)));
    }
    this.sources = Collections.unmodifiableList(newTerms);
  }

  @Override
  public final void next() throws IOException {
    LOG.trace("next()");
    if (currentTerm == null) {
      return;
    }

    // Advance currentTerm
    currentTerm.iter.next();

    // Avoid computing this multiple times
    final boolean currentTermHasMoreEntries = currentTerm.hasEntryForTerm();

    // optimization.
    // if size == 0, currentTerm is the only item left,
    // OR there are no items left.
    // In either case, we don't need to use the PriorityQueue
    if (!sorted.isEmpty()) {
      // Add the currentTerm back to the heap to let it sort it with the rest
      if (currentTermHasMoreEntries) {
        sorted.add(currentTerm);
      }
      // Let the heap return the next value to inspect
      currentTerm = sorted.poll();
    } else if (!currentTermHasMoreEntries) {
      // This currentTerm source was our last TermSource and it ran out of results
      currentTerm = null;
    } // else, currentTerm is the last TermSource and it has more results
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    LOG.trace("seek() range={}", range);
    // If sources.size is 0, there is nothing to process, so just return.
    if (sources.isEmpty()) {
      currentTerm = null;
      return;
    }

    // Optimization for when there is only one term.
    // Yes, this is lots of duplicate code, but the speed works...
    // and we don't have a priority queue of size 0 or 1.
    if (sources.size() == 1) {
      currentTerm = sources.get(0);
      currentTerm.seek(range);

      if (!currentTerm.hasEntryForTerm()) {
        // Signifies that there are no possible results for this range.
        currentTerm = null;
      }

      // Otherwise, source is valid.
      return;
    }

    // Clear the PriorityQueue so that we can re-populate it.
    sorted.clear();

    // For each term, seek forward.
    // if a hit is not found, delete it from future searches.
    for (TermSource ts : sources) {
      // Pivot the provided range into the correct range for this TermSource and seek the TS.
      ts.seek(range);

      if (ts.hasEntryForTerm()) {
        LOG.trace("Retaining TermSource for {}", ts);
        // Otherwise, source is valid. Add it to the sources.
        sorted.add(ts);
      } else {
        LOG.trace("Not adding TermSource to heap for {}", ts);
      }
    }

    // And set currentTerm = the next valid key/term.
    // If the heap is empty, it returns null which signals iteration to cease
    currentTerm = sorted.poll();
  }

  @Override
  public final Key getTopKey() {
    final Key k = currentTerm.iter.getTopKey();
    LOG.trace("getTopKey() = {}", k);
    return k;
  }

  @Override
  public final Value getTopValue() {
    final Value v = currentTerm.iter.getTopValue();
    LOG.trace("getTopValue() = {}", v);
    return v;
  }

  @Override
  public final boolean hasTop() {
    LOG.trace("hasTop()");
    return currentTerm != null;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    LOG.trace("init()");
    String columnsValue = options.get(COLUMNS_KEY);
    if (columnsValue == null) {
      throw new IllegalArgumentException(
          COLUMNS_KEY + " was not provided in the iterator configuration");
    }
    String[] columns = columnsValue.split(",");
    setTerms(source, Arrays.asList(columns), env);
    LOG.trace("Set sources: {}", this.sources);
  }

  @Override
  public IteratorOptions describeOptions() {
    Map<String,String> options = new HashMap<>();
    options.put(COLUMNS_KEY, "A comma-separated list of families");
    return new IteratorOptions("OrIterator",
        "Produces a sorted stream of qualifiers based on families", options,
        Collections.emptyList());
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    return options.get(COLUMNS_KEY) != null;
  }
}
