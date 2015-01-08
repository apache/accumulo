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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * An iterator that handles "OR" query constructs on the server side. This code has been adapted/merged from Heap and Multi Iterators.
 */

public class OrIterator implements SortedKeyValueIterator<Key,Value> {

  private TermSource currentTerm;
  private ArrayList<TermSource> sources;
  private PriorityQueue<TermSource> sorted = new PriorityQueue<TermSource>(5);
  private static final Text nullText = new Text();
  private static final Key nullKey = new Key();

  protected static final Logger log = Logger.getLogger(OrIterator.class);

  protected static class TermSource implements Comparable<TermSource> {
    public SortedKeyValueIterator<Key,Value> iter;
    public Text term;
    public Collection<ByteSequence> seekColfams;

    public TermSource(TermSource other) {
      this.iter = other.iter;
      this.term = other.term;
      this.seekColfams = other.seekColfams;
    }

    public TermSource(SortedKeyValueIterator<Key,Value> iter, Text term) {
      this.iter = iter;
      this.term = term;
      // The desired column families for this source is the term itself
      this.seekColfams = Collections.<ByteSequence> singletonList(new ArrayByteSequence(term.getBytes(), 0, term.getLength()));
    }

    public int compareTo(TermSource o) {
      // NOTE: If your implementation can have more than one row in a tablet,
      // you must compare row key here first, then column qualifier.
      // NOTE2: A null check is not needed because things are only added to the
      // sorted after they have been determined to be valid.
      return this.iter.getTopKey().compareColumnQualifier(o.iter.getTopKey().getColumnQualifier());
    }
  }

  public OrIterator() {
    this.sources = new ArrayList<TermSource>();
  }

  private OrIterator(OrIterator other, IteratorEnvironment env) {
    this.sources = new ArrayList<TermSource>();

    for (TermSource TS : other.sources)
      this.sources.add(new TermSource(TS.iter.deepCopy(env), TS.term));
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new OrIterator(this, env);
  }

  public void addTerm(SortedKeyValueIterator<Key,Value> source, Text term, IteratorEnvironment env) {
    this.sources.add(new TermSource(source.deepCopy(env), term));
  }

  @Override
  final public void next() throws IOException {

    if (currentTerm == null)
      return;

    // Advance currentTerm
    currentTerm.iter.next();

    // See if currentTerm is still valid, remove if not
    if (!(currentTerm.iter.hasTop()) || ((currentTerm.term != null) && (currentTerm.term.compareTo(currentTerm.iter.getTopKey().getColumnFamily()) != 0)))
      currentTerm = null;

    // optimization.
    // if size == 0, currentTerm is the only item left,
    // OR there are no items left.
    // In either case, we don't need to use the PriorityQueue
    if (sorted.size() > 0) {
      // sort the term back in
      if (currentTerm != null)
        sorted.add(currentTerm);
      // and get the current top item out.
      currentTerm = sorted.poll();
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

    // If sources.size is 0, there is nothing to process, so just return.
    if (sources.size() == 0) {
      currentTerm = null;
      return;
    }

    // Optimization for when there is only one term.
    // Yes, this is lots of duplicate code, but the speed works...
    // and we don't have a priority queue of size 0 or 1.
    if (sources.size() == 1) {

      if (currentTerm == null)
        currentTerm = sources.get(0);
      Range newRange = null;

      if (range != null) {
        if ((range.getStartKey() == null) || (range.getStartKey().getRow() == null))
          newRange = range;
        else {
          Key newKey = null;
          if (range.getStartKey().getColumnQualifier() == null)
            newKey = new Key(range.getStartKey().getRow(), (currentTerm.term == null) ? nullText : currentTerm.term);
          else
            newKey = new Key(range.getStartKey().getRow(), (currentTerm.term == null) ? nullText : currentTerm.term, range.getStartKey().getColumnQualifier());
          newRange = new Range((newKey == null) ? nullKey : newKey, true, range.getEndKey(), false);
        }
      }
      currentTerm.iter.seek(newRange, currentTerm.seekColfams, true);

      // If there is no top key
      // OR we are:
      // 1) NOT an iterator
      // 2) we have seeked into the next term (ie: seek man, get man001)
      // then ignore it as a valid source
      if (!(currentTerm.iter.hasTop()) || ((currentTerm.term != null) && (currentTerm.term.compareTo(currentTerm.iter.getTopKey().getColumnFamily()) != 0)))
        currentTerm = null;

      // Otherwise, source is valid.
      return;
    }

    // Clear the PriorityQueue so that we can re-populate it.
    sorted.clear();

    // This check is put in here to guard against the "initial seek"
    // crashing us because the topkey term does not match.
    // Note: It is safe to do the "sources.size() == 1" above
    // because an Or must have at least two elements.
    if (currentTerm == null) {
      for (TermSource TS : sources) {
        TS.iter.seek(range, TS.seekColfams, true);

        if ((TS.iter.hasTop()) && ((TS.term != null) && (TS.term.compareTo(TS.iter.getTopKey().getColumnFamily()) == 0)))
          sorted.add(TS);
      }
      currentTerm = sorted.poll();
      return;
    }

    TermSource TS = null;
    Iterator<TermSource> iter = sources.iterator();
    // For each term, seek forward.
    // if a hit is not found, delete it from future searches.
    while (iter.hasNext()) {
      TS = iter.next();
      Range newRange = null;

      if (range != null) {
        if ((range.getStartKey() == null) || (range.getStartKey().getRow() == null))
          newRange = range;
        else {
          Key newKey = null;
          if (range.getStartKey().getColumnQualifier() == null)
            newKey = new Key(range.getStartKey().getRow(), (TS.term == null) ? nullText : TS.term);
          else
            newKey = new Key(range.getStartKey().getRow(), (TS.term == null) ? nullText : TS.term, range.getStartKey().getColumnQualifier());
          newRange = new Range((newKey == null) ? nullKey : newKey, true, range.getEndKey(), false);
        }
      }

      // Seek only to the term for this source as a column family
      TS.iter.seek(newRange, TS.seekColfams, true);

      // If there is no top key
      // OR we are:
      // 1) NOT an iterator
      // 2) we have seeked into the next term (ie: seek man, get man001)
      // then ignore it as a valid source
      if (!(TS.iter.hasTop()) || ((TS.term != null) && (TS.term.compareTo(TS.iter.getTopKey().getColumnFamily()) != 0)))
        iter.remove();

      // Otherwise, source is valid. Add it to the sources.
      sorted.add(TS);
    }

    // And set currentTerm = the next valid key/term.
    currentTerm = sorted.poll();
  }

  @Override
  final public Key getTopKey() {
    return currentTerm.iter.getTopKey();
  }

  @Override
  final public Value getTopValue() {
    return currentTerm.iter.getTopValue();
  }

  @Override
  final public boolean hasTop() {
    return currentTerm != null;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }
}
