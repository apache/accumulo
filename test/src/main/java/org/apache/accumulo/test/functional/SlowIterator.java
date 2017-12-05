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

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

public class SlowIterator extends WrappingIterator {

  static private final String SLEEP_TIME = "sleepTime";
  static private final String SEEK_SLEEP_TIME = "seekSleepTime";

  private long sleepTime = 0;
  private long seekSleepTime = 0;

  public static void setSleepTime(IteratorSetting is, long millis) {
    is.addOption(SLEEP_TIME, Long.toString(millis));
  }

  public static void setSeekSleepTime(IteratorSetting is, long t) {
    is.addOption(SEEK_SLEEP_TIME, Long.toString(t));
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void next() throws IOException {
    sleepUninterruptibly(sleepTime, TimeUnit.MILLISECONDS);
    super.next();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    sleepUninterruptibly(seekSleepTime, TimeUnit.MILLISECONDS);
    super.seek(range, columnFamilies, inclusive);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(SLEEP_TIME))
      sleepTime = Long.parseLong(options.get(SLEEP_TIME));

    if (options.containsKey(SEEK_SLEEP_TIME))
      seekSleepTime = Long.parseLong(options.get(SEEK_SLEEP_TIME));
  }

}
