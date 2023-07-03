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
package org.apache.accumulo.test.functional;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

import com.google.common.base.Preconditions;

/**
 * Iterator used in tests *and* the test class must spawn a new MAC instance for each test since the
 * timesThrown variable is static.
 */
public class ErrorThrowingIterator extends WrappingIterator {

  public static final String TIMES = "error.throwing.iterator.times";

  private static final String MESSAGE = "Exception thrown from ErrorThrowingIterator";
  private static final RuntimeException ERROR = new RuntimeException(MESSAGE);
  private static final AtomicInteger TIMES_THROWN = new AtomicInteger(0);

  private int threshold = 0;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    threshold = Integer.parseInt(options.get(TIMES));
    Preconditions.checkState(TIMES_THROWN.get() <= threshold,
        "This iterator does not"
            + " support reuse within the same VM. If using in an IT, then be sure to use"
            + " a different MAC instance between tests.");
  }

  private void incrementAndThrow(RuntimeException t) {
    if (TIMES_THROWN.get() < threshold) {
      TIMES_THROWN.incrementAndGet();
      throw t;
    }
  }

  private void incrementAndThrowIOE() throws IOException {
    if (TIMES_THROWN.get() < threshold) {
      TIMES_THROWN.incrementAndGet();
      throw new IOException(MESSAGE);
    }
  }

  @Override
  public Key getTopKey() {
    incrementAndThrow(ERROR);
    return super.getTopKey();
  }

  @Override
  public Value getTopValue() {
    incrementAndThrow(ERROR);
    return super.getTopValue();
  }

  @Override
  public boolean hasTop() {
    incrementAndThrow(ERROR);
    return super.hasTop();
  }

  @Override
  public void next() throws IOException {
    incrementAndThrowIOE();
    super.next();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    incrementAndThrowIOE();
    super.seek(range, columnFamilies, inclusive);
  }

}
