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
 * This iterator provides exact string matching of the term specified by the "term" option. It
 * searches both the Key and Value for the string, according to the specified configuration options.
 *
 * <p>
 * The match options are:
 * <ul>
 * <li>term (String, required)
 * <li>matchRow (boolean, default: true)
 * <li>matchColumnFamily (boolean, default: true)
 * <li>matchColumnQualifier (boolean, default: true)
 * <li>matchColumnVisibility (boolean, default: false)
 * <li>matchValue (boolean, default: true)
 * </ul>
 */
public class GrepIterator extends Filter {

  private static final String TERM_OPT = "term";

  private static final String MATCH_ROW_OPT = "matchRow";
  private static final String MATCH_COLFAM_OPT = "matchColumnFamily";
  private static final String MATCH_COLQUAL_OPT = "matchColumnQualifier";
  private static final String MATCH_COLVIS_OPT = "matchColumnVisibility";
  private static final String MATCH_VALUE_OPT = "matchValue";

  private byte[] term;
  private int[] right = new int[256];

  private boolean matchRow = true;
  private boolean matchColFam = true;
  private boolean matchColQual = true;
  private boolean matchColVis = false;
  private boolean matchValue = true;

  @Override
  public boolean accept(Key k, Value v) {
    return (matchValue && match(v.get())) || (matchRow && match(k.getRowData()))
        || (matchColFam && match(k.getColumnFamilyData()))
        || (matchColQual && match(k.getColumnQualifierData()))
        || (matchColVis && match(k.getColumnVisibilityData()));
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
    copy.matchRow = matchRow;
    copy.matchColFam = matchColFam;
    copy.matchColQual = matchColQual;
    copy.matchColVis = matchColVis;
    copy.matchValue = matchValue;
    return copy;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    term = options.get(TERM_OPT).getBytes(UTF_8);
    for (int i = 0; i < right.length; i++) {
      right[i] = -1;
    }
    for (int i = 0; i < term.length; i++) {
      right[term[i] & 0xff] = i;
    }
    String matchItem = null;
    if ((matchItem = options.get(MATCH_ROW_OPT)) != null) {
      matchRow = Boolean.parseBoolean(matchItem);
    }
    if ((matchItem = options.get(MATCH_COLFAM_OPT)) != null) {
      matchColFam = Boolean.parseBoolean(matchItem);
    }
    if ((matchItem = options.get(MATCH_COLQUAL_OPT)) != null) {
      matchColQual = Boolean.parseBoolean(matchItem);
    }
    if ((matchItem = options.get(MATCH_COLVIS_OPT)) != null) {
      matchColVis = Boolean.parseBoolean(matchItem);
    }
    if ((matchItem = options.get(MATCH_VALUE_OPT)) != null) {
      matchValue = Boolean.parseBoolean(matchItem);
    }
  }

  /**
   * Encode the grep term as an option for a ScanIterator
   */
  public static void setTerm(IteratorSetting cfg, String term) {
    cfg.addOption(TERM_OPT, term);
  }

  public static void matchRow(IteratorSetting cfg, boolean match) {
    cfg.addOption(MATCH_ROW_OPT, Boolean.toString(match));
  }

  public static void matchColumnFamily(IteratorSetting cfg, boolean match) {
    cfg.addOption(MATCH_COLFAM_OPT, Boolean.toString(match));
  }

  public static void matchColumnQualifier(IteratorSetting cfg, boolean match) {
    cfg.addOption(MATCH_COLQUAL_OPT, Boolean.toString(match));
  }

  public static void matchColumnVisibility(IteratorSetting cfg, boolean match) {
    cfg.addOption(MATCH_COLVIS_OPT, Boolean.toString(match));
  }

  public static void matchValue(IteratorSetting cfg, boolean match) {
    cfg.addOption(MATCH_VALUE_OPT, Boolean.toString(match));
  }
}
