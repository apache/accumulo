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

import static java.nio.charset.StandardCharsets.UTF_8;

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
 * This iterator provides exact string matching. It searches both the Key and Value for the string.
 * The string to match is specified by the "term" option.
 */
public class GrepIterator extends Filter {

  private byte[] term;
  private int[] right = new int[256];

  @Override
  public boolean accept(Key k, Value v) {
    return match(v.get()) || match(k.getRowData()) || match(k.getColumnFamilyData())
        || match(k.getColumnQualifierData());
  }

  protected boolean match(ByteSequence bs) {
    return indexOf(bs.getBackingArray(), bs.offset(), bs.length()) >= 0;
  }

  protected boolean match(byte[] ba) {
    return indexOf(ba, 0, ba.length) >= 0;
  }

  protected int indexOf(byte[] value, int offset, int length) {
    final int M = term.length;
    final int N = offset + length;
    int skip;
    for (int i = offset; i <= N - M; i += skip) {
      skip = 0;
      for (int j = M - 1; j >= 0; j--) {
        if (term[j] != value[i + j]) {
          skip = Math.max(1, j - right[value[i + j] & 0xff]);
        }
      }
      if (skip == 0) {
        return i;
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
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    term = options.get("term").getBytes(UTF_8);
    for (int i = 0; i < right.length; i++) {
      right[i] = -1;
    }
    for (int i = 0; i < term.length; i++) {
      right[term[i] & 0xff] = i;
    }
  }

  /**
   * Encode the grep term as an option for a ScanIterator
   */
  public static void setTerm(IteratorSetting cfg, String term) {
    cfg.addOption("term", term);
  }
}
