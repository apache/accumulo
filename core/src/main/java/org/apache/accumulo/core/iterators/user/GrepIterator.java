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
package org.apache.accumulo.core.iterators.user;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * This iterator provides exact string matching. It searches both the Key and Value for the string. The string to match is specified by the "term" option.
 */
public class GrepIterator extends Filter {

  private byte term[];

  @Override
  public boolean accept(Key k, Value v) {
    return match(v.get()) || match(k.getRowData()) || match(k.getColumnFamilyData()) || match(k.getColumnQualifierData());
  }

  private boolean match(ByteSequence bs) {
    return indexOf(bs.getBackingArray(), bs.offset(), bs.length(), term) >= 0;
  }

  private boolean match(byte[] ba) {
    return indexOf(ba, 0, ba.length, term) >= 0;
  }

  // copied code below from java string and modified

  private static int indexOf(byte[] source, int sourceOffset, int sourceCount, byte[] target) {
    byte first = target[0];
    int targetCount = target.length;
    int max = sourceOffset + (sourceCount - targetCount);

    for (int i = sourceOffset; i <= max; i++) {
      /* Look for first character. */
      if (source[i] != first) {
        while (++i <= max && source[i] != first)
          continue;
      }

      /* Found first character, now look at the rest of v2 */
      if (i <= max) {
        int j = i + 1;
        int end = j + targetCount - 1;
        for (int k = 1; j < end && source[j] == target[k]; j++, k++)
          continue;

        if (j == end) {
          /* Found whole string. */
          return i - sourceOffset;
        }
      }
    }
    return -1;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    GrepIterator copy = (GrepIterator) super.deepCopy(env);
    copy.term = Arrays.copyOf(term, term.length);
    return copy;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    term = options.get("term").getBytes(UTF_8);
  }

  /**
   * Encode the grep term as an option for a ScanIterator
   */
  public static void setTerm(IteratorSetting cfg, String term) {
    cfg.addOption("term", term);
  }
}
