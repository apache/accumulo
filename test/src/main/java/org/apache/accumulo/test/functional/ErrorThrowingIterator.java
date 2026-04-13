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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Iterator used in tests *and* the test class must spawn a new MAC instance for each test since the
 * timesThrown variable is static.
 */
public class ErrorThrowingIterator extends WrappingIterator {

  private static final Logger log = LoggerFactory.getLogger(ErrorThrowingIterator.class);

  public static final String TIMES = "error.throwing.iterator.times";
  public static final String NAME = "error.throwing.iterator.name";
  public static final String ROW = "error.throwing.iterator.row";

  private static final String MESSAGE = "Exception thrown from ErrorThrowingIterator";
  private static final RuntimeException ERROR = new RuntimeException(MESSAGE);
  private static final Map<String,AtomicInteger> TIMES_THROWN = new ConcurrentHashMap<>();

  private int threshold = 0;
  private String name;
  private String row;

  private static AtomicInteger getCounter(String name) {
    return TIMES_THROWN.computeIfAbsent(name, n -> new AtomicInteger());
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    threshold = Integer.parseInt(options.get(TIMES));
    name = options.getOrDefault(NAME, "");
    row = options.getOrDefault(ROW, null);

    Preconditions.checkState(getCounter(name).get() <= threshold,
        "This iterator does not support reuse within the same VM (name='" + name
            + "'). If using in an IT, then be sure to use"
            + " a different MAC instance between tests or set a different name.");
  }

  private void incrementAndThrow(RuntimeException t) {
    var counter = getCounter(name);
    if (counter.get() < threshold) {
      counter.incrementAndGet();
      log.info("Throwing {}", t.getClass().getName());
      throw t;
    }
  }

  private void incrementAndThrowIOE() throws IOException {
    var counter = getCounter(name);
    if (counter.get() < threshold) {
      counter.incrementAndGet();
      log.info("Throwing IOException");
      throw new IOException(MESSAGE);
    }
  }

  @Override
  public Key getTopKey() {
    if (row == null) {
      incrementAndThrow(ERROR);
    }
    return super.getTopKey();
  }

  @Override
  public Value getTopValue() {
    if (row == null) {
      incrementAndThrow(ERROR);
    }
    return super.getTopValue();
  }

  @Override
  public boolean hasTop() {
    if (row == null) {
      incrementAndThrow(ERROR);
    }
    return super.hasTop();
  }

  @Override
  public void next() throws IOException {
    if (row == null || super.getTopKey().getRowData().toString().equals(row)) {
      incrementAndThrowIOE();
    }
    super.next();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    if (row == null) {
      incrementAndThrowIOE();
    }
    super.seek(range, columnFamilies, inclusive);
  }

}
