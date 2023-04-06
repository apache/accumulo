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
import java.util.Collection;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

/**
 * An optimized version of {@link org.apache.accumulo.core.iterators.SkippingIterator}. This class
 * grants protected access to the read only <code>source</code> iterator. For performance reasons,
 * the <code>source</code> iterator is declared final and subclasses directly access it, no longer
 * requiring calls to getSource().
 *
 * @since 2.0
 */
public abstract class ServerSkippingIterator extends ServerWrappingIterator {

  public ServerSkippingIterator(SortedKeyValueIterator<Key,Value> source) {
    super(source);
  }

  @Override
  public void next() throws IOException {
    source.next();
    consume();
  }

  protected abstract void consume() throws IOException;

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    source.seek(range, columnFamilies, inclusive);
    consume();
  }

}
