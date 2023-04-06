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
package org.apache.accumulo.core.iterators.user;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This iterator facilitates document-partitioned indexing. It is an example of extending the
 * IntersectingIterator to customize the placement of the term and docID. As with the
 * IntersectingIterator, documents are grouped together and indexed into a single row of an Accumulo
 * table. This allows a tablet server to perform boolean AND operations on terms in the index. This
 * iterator also stores the document contents in a separate column family in the same row so that
 * the full document can be returned with each query.
 *
 * The table structure should have the following form:
 *
 * row: shardID, colfam: docColf\0doctype, colqual: docID, value: doc
 *
 * row: shardID, colfam: indexColf, colqual: term\0doctype\0docID\0info, value: (empty)
 *
 * When you configure this iterator with a set of terms, it will return only the docIDs and docs
 * that appear with all of the specified terms. The result will have the following form:
 *
 * row: shardID, colfam: indexColf, colqual: doctype\0docID\0info, value: doc
 *
 * This iterator is commonly used with BatchScanner to parallelize the search over all shardIDs.
 */
public class IndexedDocIterator extends IntersectingIterator {
  private static final Logger log = LoggerFactory.getLogger(IndexedDocIterator.class);
  public static final Text DEFAULT_INDEX_COLF = new Text("i");
  public static final Text DEFAULT_DOC_COLF = new Text("e");

  private static final String indexFamilyOptionName = "indexFamily";
  private static final String docFamilyOptionName = "docFamily";

  private Text indexColf = DEFAULT_INDEX_COLF;
  private Text docColf = DEFAULT_DOC_COLF;
  private Set<ByteSequence> indexColfSet;
  private Set<ByteSequence> docColfSet;

  private static final byte[] nullByte = {0};

  public SortedKeyValueIterator<Key,Value> docSource;

  @Override
  protected Key buildKey(Text partition, Text term, Text docID) {
    Text colq = new Text(term);
    colq.append(nullByte, 0, 1);
    colq.append(docID.getBytes(), 0, docID.getLength());
    colq.append(nullByte, 0, 1);
    return new Key(partition, indexColf, colq);
  }

  @Override
  protected Key buildKey(Text partition, Text term) {
    Text colq = new Text(term);
    return new Key(partition, indexColf, colq);
  }

  @Override
  protected Text getDocID(Key key) {
    return parseDocID(key);
  }

  public static Text parseDocID(Key key) {
    Text colq = key.getColumnQualifier();
    int firstZeroIndex = colq.find("\0");
    if (firstZeroIndex < 0) {
      throw new IllegalArgumentException("bad docid: " + key);
    }
    int secondZeroIndex = colq.find("\0", firstZeroIndex + 1);
    if (secondZeroIndex < 0) {
      throw new IllegalArgumentException("bad docid: " + key);
    }
    int thirdZeroIndex = colq.find("\0", secondZeroIndex + 1);
    if (thirdZeroIndex < 0) {
      throw new IllegalArgumentException("bad docid: " + key);
    }
    Text docID = new Text();
    try {
      docID.set(colq.getBytes(), firstZeroIndex + 1, thirdZeroIndex - 1 - firstZeroIndex);
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new IllegalArgumentException("bad indices for docid: " + key + " " + firstZeroIndex
          + " " + secondZeroIndex + " " + thirdZeroIndex);
    }
    return docID;
  }

  @Override
  protected Text getTerm(Key key) {
    if (indexColf.compareTo(key.getColumnFamily().getBytes(), 0, indexColf.getLength()) < 0) {
      // We're past the index column family, so return a term that will sort lexicographically last.
      // The last unicode character should suffice
      return new Text("\uFFFD");
    }
    Text colq = key.getColumnQualifier();
    int zeroIndex = colq.find("\0");
    Text term = new Text();
    term.set(colq.getBytes(), 0, zeroIndex);
    return term;
  }

  @Override
  public synchronized void init(SortedKeyValueIterator<Key,Value> source,
      Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(indexFamilyOptionName)) {
      indexColf = new Text(options.get(indexFamilyOptionName));
    }
    if (options.containsKey(docFamilyOptionName)) {
      docColf = new Text(options.get(docFamilyOptionName));
    }
    docSource = source.deepCopy(env);
    indexColfSet = Collections
        .singleton(new ArrayByteSequence(indexColf.getBytes(), 0, indexColf.getLength()));

    for (TermSource ts : this.sources) {
      ts.seekColfams = indexColfSet;
    }
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> seekColumnFamilies, boolean inclusive)
      throws IOException {
    super.seek(range, null, true);

  }

  @Override
  protected void advanceToIntersection() throws IOException {
    super.advanceToIntersection();
    if (topKey == null) {
      return;
    }
    if (log.isTraceEnabled()) {
      log.trace("using top key to seek for doc: {}", topKey);
    }
    Key docKey = buildDocKey();
    docSource.seek(new Range(docKey, true, null, false), docColfSet, true);
    log.debug("got doc key: {}", docSource.getTopKey());
    if (docSource.hasTop() && docKey.equals(docSource.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL)) {
      value = docSource.getTopValue();
    }
    log.debug("got doc value: {}", value);
  }

  protected Key buildDocKey() {
    if (log.isTraceEnabled()) {
      log.trace("building doc key for {} {}", currentPartition, currentDocID);
    }
    int zeroIndex = currentDocID.find("\0");
    if (zeroIndex < 0) {
      throw new IllegalArgumentException("bad current docID");
    }
    Text colf = new Text(docColf);
    colf.append(nullByte, 0, 1);
    colf.append(currentDocID.getBytes(), 0, zeroIndex);
    docColfSet = Collections.singleton(new ArrayByteSequence(colf.getBytes(), 0, colf.getLength()));
    if (log.isTraceEnabled()) {
      log.trace("{} {}", zeroIndex, currentDocID.getLength());
    }
    Text colq = new Text();
    colq.set(currentDocID.getBytes(), zeroIndex + 1, currentDocID.getLength() - zeroIndex - 1);
    Key k = new Key(currentPartition, colf, colq);
    if (log.isTraceEnabled()) {
      log.trace("built doc key for seek: {}", k);
    }
    return k;
  }

  /**
   * A convenience method for setting the index column family.
   *
   * @param is IteratorSetting object to configure.
   * @param indexColf the index column family
   */
  public static void setIndexColf(IteratorSetting is, String indexColf) {
    is.addOption(indexFamilyOptionName, indexColf);
  }

  /**
   * A convenience method for setting the document column family prefix.
   *
   * @param is IteratorSetting object to configure.
   * @param docColfPrefix the prefix of the document column family (colf will be of the form
   *        docColfPrefix\0doctype)
   */
  public static void setDocColfPrefix(IteratorSetting is, String docColfPrefix) {
    is.addOption(docFamilyOptionName, docColfPrefix);
  }

  /**
   * A convenience method for setting the index column family and document column family prefix.
   *
   * @param is IteratorSetting object to configure.
   * @param indexColf the index column family
   * @param docColfPrefix the prefix of the document column family (colf will be of the form
   *        docColfPrefix\0doctype)
   */
  public static void setColfs(IteratorSetting is, String indexColf, String docColfPrefix) {
    setIndexColf(is, indexColf);
    setDocColfPrefix(is, docColfPrefix);
  }
}
