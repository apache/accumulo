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

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;

/**
 * Every call to {@link #next()} and {@link #seek(Range, Collection, boolean)} calls the parent's implementation and then calls the implementation's
 * {@link #consume()}. This provides a cleaner way for implementers to write code that will read one to many key-value pairs, as opposed to returning every
 * key-value pair seen.
 */
public abstract class SkippingIterator extends WrappingIterator {

  @Override
  public void next() throws IOException {
    super.next();
    consume();
  }

  protected abstract void consume() throws IOException;

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    super.seek(range, columnFamilies, inclusive);
    consume();
  }

}
