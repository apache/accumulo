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
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * An iterator that supports iterating over key and value pairs. Anything implementing this interface should return keys in sorted order.
 */

public interface SortedKeyValueIterator<K extends WritableComparable<?>,V extends Writable> {
  /**
   * Initializes the iterator. Data should not be read from the source in this method.
   *
   * @param source
   *          <tt>SortedKeyValueIterator</tt> source to read data from.
   * @param options
   *          <tt>Map</tt> map of string option names to option values.
   * @param env
   *          <tt>IteratorEnvironment</tt> environment in which iterator is being run.
   * @throws IOException
   *           unused.
   * @exception IllegalArgumentException
   *              if there are problems with the options.
   * @exception UnsupportedOperationException
   *              if not supported.
   */
  void init(SortedKeyValueIterator<K,V> source, Map<String,String> options, IteratorEnvironment env) throws IOException;

  /**
   * Returns true if the iterator has more elements.
   *
   * @return <tt>true</tt> if the iterator has more elements.
   * @exception IllegalStateException
   *              if called before seek.
   */
  boolean hasTop();

  /**
   * Advances to the next K,V pair. Note that in minor compaction scope and in non-full major compaction scopes the iterator may see deletion entries. These
   * entries should be preserved by all iterators except ones that are strictly scan-time iterators that will never be configured for the minc or majc scopes.
   * Deletion entries are only removed during full major compactions.
   *
   * @throws IOException
   *           if an I/O error occurs.
   * @exception IllegalStateException
   *              if called before seek.
   * @exception NoSuchElementException
   *              if next element doesn't exist.
   */
  void next() throws IOException;

  /**
   * Seeks to the first key in the Range, restricting the resulting K,V pairs to those with the specified columns. An iterator does not have to stop at the end
   * of the range. The whole range is provided so that iterators can make optimizations.
   *
   * Seek may be called multiple times with different parameters after {@link #init} is called.
   *
   * Iterators that examine groups of adjacent key/value pairs (e.g. rows) to determine their top key and value should be sure that they properly handle a seek
   * to a key in the middle of such a group (e.g. the middle of a row). Even if the client always seeks to a range containing an entire group (a,c), the tablet
   * server could send back a batch of entries corresponding to (a,b], then reseek the iterator to range (b,c) when the scan is continued.
   *
   * {@code columnFamilies} is used, at the lowest level, to determine which data blocks inside of an RFile need to be opened for this iterator. This set of
   * data blocks is also the set of locality groups defined for the given table. If no columnFamilies are provided, the data blocks for all locality groups
   * inside of the correct RFile will be opened and seeked in an attempt to find the correct start key, regardless of the startKey in the {@code range}.
   *
   * In an Accumulo instance in which multiple locality groups exist for a table, it is important to ensure that {@code columnFamilies} is properly set to the
   * minimum required column families to ensure that data from separate locality groups is not inadvertently read.
   *
   * @param range
   *          <tt>Range</tt> of keys to iterate over.
   * @param columnFamilies
   *          <tt>Collection</tt> of column families to include or exclude.
   * @param inclusive
   *          <tt>boolean</tt> that indicates whether to include (true) or exclude (false) column families.
   * @throws IOException
   *           if an I/O error occurs.
   * @exception IllegalArgumentException
   *              if there are problems with the parameters.
   */
  void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException;

  /**
   * Returns top key. Can be called 0 or more times without affecting behavior of next() or hasTop(). Note that in minor compaction scope and in non-full major
   * compaction scopes the iterator may see deletion entries. These entries should be preserved by all iterators except ones that are strictly scan-time
   * iterators that will never be configured for the minc or majc scopes. Deletion entries are only removed during full major compactions.
   *
   * @return <tt>K</tt>
   * @exception IllegalStateException
   *              if called before seek.
   * @exception NoSuchElementException
   *              if top element doesn't exist.
   */
  K getTopKey();

  /**
   * Returns top value. Can be called 0 or more times without affecting behavior of next() or hasTop().
   *
   * @return <tt>V</tt>
   * @exception IllegalStateException
   *              if called before seek.
   * @exception NoSuchElementException
   *              if top element doesn't exist.
   */
  V getTopValue();

  /**
   * Creates a deep copy of this iterator as though seek had not yet been called. init should be called on an iterator before deepCopy is called. init should
   * not need to be called on the copy that is returned by deepCopy; that is, when necessary init should be called in the deepCopy method on the iterator it
   * returns. The behavior is unspecified if init is called after deepCopy either on the original or the copy.
   *
   * @param env
   *          <tt>IteratorEnvironment</tt> environment in which iterator is being run.
   * @return <tt>SortedKeyValueIterator</tt> a copy of this iterator (with the same source and settings).
   * @exception UnsupportedOperationException
   *              if not supported.
   */
  SortedKeyValueIterator<K,V> deepCopy(IteratorEnvironment env);
}
