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
package org.apache.accumulo.test.functional;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

public class SlowButBusyIterator extends WrappingIterator {

  static private final String MAX_COUNT = "maxcounter";
  static private final String LOOPS = "loops";

  private long maxCount = 0;
  private long maxLoops = 1;

  public static void setMaxCount(IteratorSetting is, long millis) {
    is.addOption(MAX_COUNT, Long.toString(millis));
  }

  public static void setNumberLoops(IteratorSetting is, long t) {
    is.addOption(LOOPS, Long.toString(t));
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  public static void sleepByCounting(long count, long loops) {
    long countr = 0;
    Random rand = new Random();
    for (long i = 0; i < loops; i++) {
      for (long j = 0; j < count; j++) {
        countr *= rand.nextInt();
        countr += rand.nextInt();
      }
    }
    countr = 0;
  }

  @Override
  public void next() throws IOException {
    sleepByCounting(maxCount, maxLoops);
    super.next();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    sleepByCounting(maxCount, maxLoops);
    super.seek(range, columnFamilies, inclusive);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(MAX_COUNT))
      maxCount = Long.parseLong(options.get(MAX_COUNT));

    if (options.containsKey(LOOPS))
      maxLoops = Long.parseLong(options.get(LOOPS));
  }

}
