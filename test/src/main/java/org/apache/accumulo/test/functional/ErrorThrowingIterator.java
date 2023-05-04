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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

/**
 * Test iterator that throws Errors (vs Exceptions) to simulate "unexpected" conditions.
 */
public class ErrorThrowingIterator extends WrappingIterator {

  private static final String THROW_ERROR_ON_INIT = "throwErrorOnInit";
  private static final String THROW_ERROR_ON_NEXT = "throwErrorOnNext";
  private static final String THROW_ERROR_ON_SEEK = "throwErrorOnSeek";

  public static void setThrowErrorOnInit(IteratorSetting is) {
    is.addOption(THROW_ERROR_ON_INIT, Boolean.TRUE.toString());
  }

  public static void setThrowErrorOnNext(IteratorSetting is) {
    is.addOption(THROW_ERROR_ON_NEXT, Boolean.TRUE.toString());
  }

  public static void setThrowErrorOnSeek(IteratorSetting is) {
    is.addOption(THROW_ERROR_ON_SEEK, Boolean.TRUE.toString());
  }

  private boolean throwErrorOnNext = false;
  private boolean throwErrorOnSeek = false;

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void next() throws IOException {
    if (throwErrorOnNext) {
      throw new Error("test error explicitly thrown in next() method");
    }
    super.next();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    if (throwErrorOnSeek) {
      throw new Error("test error explicitly thrown in seek() method");
    }
    super.seek(range, columnFamilies, inclusive);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(THROW_ERROR_ON_INIT)) {
      boolean throwErrorOnInit = Boolean.parseBoolean(options.get(THROW_ERROR_ON_INIT));
      if (throwErrorOnInit) {
        throw new Error("test error explicitly thrown in init() method");
      }
    }

    if (options.containsKey(THROW_ERROR_ON_NEXT)) {
      throwErrorOnNext = Boolean.parseBoolean(options.get(THROW_ERROR_ON_NEXT));
    }
    if (options.containsKey(THROW_ERROR_ON_SEEK)) {
      throwErrorOnSeek = Boolean.parseBoolean(options.get(THROW_ERROR_ON_SEEK));
    }
  }

}
