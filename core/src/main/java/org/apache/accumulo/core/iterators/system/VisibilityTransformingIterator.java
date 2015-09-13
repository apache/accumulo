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
package org.apache.accumulo.core.iterators.system;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public abstract class VisibilityTransformingIterator implements SortedKeyValueIterator<Key,Value> {

  private static final Logger logger = LoggerFactory.getLogger(VisibilityTransformingIterator.class);

  private SortedKeyValueIterator<Key,Value> source;
  private LinkedList<Map.Entry<Key,Value>> sourceKvPairs = new LinkedList<>();
  private NavigableMap<Key,Value> vtiKvPairs = new TreeMap<>();

  private Map.Entry<Key,Value> topEntry;
  private Range seekRange;

  private final Text rowHolder = new Text();
  private final Text cfHolder = new Text();
  private final Text cqHolder = new Text();

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
  }

  @Override
  public boolean hasTop() {
    return topEntry != null;
  }

  @Override
  public Key getTopKey() {
    return topEntry.getKey();
  }

  @Override
  public Value getTopValue() {
    return topEntry.getValue();
  }

  @Override
  public void next() throws IOException {
    if (sourceKvPairs.isEmpty() && vtiKvPairs.isEmpty()) {
      consumeSource();
    }
    setTop();
  }

  private void consumeSource() throws IOException {
    if (!source.hasTop()) {
      return;
    }
    Key sourceTop = source.getTopKey();
    Key nextKey = sourceTop.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
    while (source.hasTop() && source.getTopKey().compareTo(nextKey) < 0 && seekRange.contains(source.getTopKey())) {
      sourceKvPairs.add(new AbstractMap.SimpleImmutableEntry<>(source.getTopKey(), source.getTopValue()));
      source.next();
    }
    for (Map.Entry<Key,Value> sourceKv : sourceKvPairs) {
      Key sourceKey = sourceKv.getKey();
      for (Map.Entry<ColumnVisibility,Value> transformed : transformVisibility(sourceKey, sourceKv.getValue())) {
        Key transformedKey = replaceVisibility(sourceKey, transformed.getKey());
        if (seekRange.contains(transformedKey)) {
          vtiKvPairs.put(transformedKey, transformed.getValue());
        }
      }
    }
    setTop();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    seekRange = range;
    Range adjusted = range;
    if (!range.isInfiniteStartKey()) {
      // The start key of the range might be a transformed key, that is, it might not be present in the source
      // iterator. Seek to the specified (row, cf, cq) and start transforming from there, throwing out anything
      // prior to the specified start key. Complicated a little bit by the fact that visibility sorts before
      // timestamp -- we might have to throw out a bunch of different versions of the key before we start
      // producing useful data.
      Key startKey = range.getStartKey();
      startKey = new Key(startKey.getRow(rowHolder), startKey.getColumnFamily(cfHolder), startKey.getColumnQualifier(cqHolder));
      adjusted = new Range(startKey, range.getEndKey());
    }
    source.seek(adjusted, columnFamilies, inclusive);
    consumeSource();
    if (!adjusted.equals(range)) {
      while (getTopKey().compareTo(range.getStartKey()) < 0) {
        next();
      }
    }
  }

  private void setTop() {
    topEntry = null;
    if (vtiKvPairs.isEmpty() && sourceKvPairs.isEmpty()) {
      // exhausted.
      return;
    }
    if (vtiKvPairs.isEmpty()) {
      topEntry = sourceKvPairs.removeFirst();
    } else if (sourceKvPairs.isEmpty()) {
      topEntry = vtiKvPairs.pollFirstEntry();
    } else {
      Map.Entry<Key,Value> sourceTop = sourceKvPairs.getFirst();
      Map.Entry<Key,Value> vtiTop = vtiKvPairs.firstEntry();
      int cmp = sourceTop.getKey().compareTo(vtiTop.getKey());
      if (cmp < 0) {
        topEntry = sourceKvPairs.removeFirst();
      } else if (cmp > 0) {
        topEntry = vtiKvPairs.pollFirstEntry();
      } else {
        logger.info("Transform key " + vtiTop.getKey() + " also exists in source");
        topEntry = sourceKvPairs.removeFirst();
        vtiKvPairs.pollFirstEntry();
      }
    }
  }

  private Key replaceVisibility(Key key, ColumnVisibility newVis) {
    return new Key(key.getRow(rowHolder), key.getColumnFamily(cfHolder), key.getColumnQualifier(cqHolder), newVis, key.getTimestamp());
  }

  public static ColumnVisibility replaceTerm(ColumnVisibility vis, String oldTerm, String newTerm) {
    newTerm = ColumnVisibility.quote(newTerm);
    ByteSequence oldTermBs = new ArrayByteSequence(oldTerm.getBytes());
    ColumnVisibility.Node root = vis.getParseTree();
    byte[] expression = vis.getExpression();
    StringBuilder out = new StringBuilder();
    stringify(newTerm, oldTermBs, root, expression, out);
    return new ColumnVisibility(out.toString());
  }

  private static void stringify(String newTerm, ByteSequence oldTermBs, ColumnVisibility.Node root, byte[] expression, StringBuilder out) {
    if (root.getType() == ColumnVisibility.NodeType.TERM) {
      ByteSequence termBs = root.getTerm(expression);
      if (termBs.compareTo(oldTermBs) == 0) {
        out.append(newTerm);
      } else {
        out.append(termBs);
      }
    } else {
      String sep = "";
      for (ColumnVisibility.Node c : root.getChildren()) {
        out.append(sep);
        boolean parens = (c.getType() != ColumnVisibility.NodeType.TERM && root.getType() != c.getType());
        if (parens)
          out.append("(");
        stringify(newTerm, oldTermBs, c, expression, out);
        if (parens)
          out.append(")");
        sep = root.getType() == ColumnVisibility.NodeType.AND ? "&" : "|";
      }
    }
  }

  protected abstract Collection<? extends Map.Entry<ColumnVisibility,Value>> transformVisibility(Key key, Value value);

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    VisibilityTransformingIterator vtiIter;
    try {
      vtiIter = getClass().newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Subclasses of VisibilityTransformingIterator must define a no-arg constructor", e);
    }
    vtiIter.source = source.deepCopy(env);
    return vtiIter;
  }
}
