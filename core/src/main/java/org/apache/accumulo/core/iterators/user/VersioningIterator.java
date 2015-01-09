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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

public class VersioningIterator extends WrappingIterator implements OptionDescriber {
  private final int maxCount = 10;

  private Key currentKey = new Key();
  private int numVersions;
  protected int maxVersions;

  private Range range;
  private Collection<ByteSequence> columnFamilies;
  private boolean inclusive;

  @Override
  public VersioningIterator deepCopy(IteratorEnvironment env) {
    VersioningIterator copy = new VersioningIterator();
    copy.setSource(getSource().deepCopy(env));
    copy.maxVersions = maxVersions;
    return copy;
  }

  @Override
  public void next() throws IOException {
    if (numVersions >= maxVersions) {
      skipRowColumn();
      resetVersionCount();
      return;
    }

    super.next();
    if (getSource().hasTop()) {
      if (getSource().getTopKey().equals(currentKey, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
        numVersions++;
      } else {
        resetVersionCount();
      }
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    // do not want to seek to the middle of a row
    Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);
    this.range = seekRange;
    this.columnFamilies = columnFamilies;
    this.inclusive = inclusive;

    super.seek(seekRange, columnFamilies, inclusive);
    resetVersionCount();

    if (range.getStartKey() != null)
      while (hasTop() && range.beforeStartKey(getTopKey()))
        next();
  }

  private void resetVersionCount() {
    if (super.hasTop())
      currentKey.set(getSource().getTopKey());
    numVersions = 1;
  }

  private void skipRowColumn() throws IOException {
    Key keyToSkip = currentKey;
    super.next();

    int count = 0;
    while (getSource().hasTop() && getSource().getTopKey().equals(keyToSkip, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      if (count < maxCount) {
        // it is quicker to call next if we are close, but we never know if we are close
        // so give next a try a few times
        getSource().next();
        count++;
      } else {
        reseek(keyToSkip.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
        count = 0;
      }
    }
  }

  protected void reseek(Key key) throws IOException {
    if (key == null)
      return;
    if (range.afterEndKey(key)) {
      range = new Range(range.getEndKey(), true, range.getEndKey(), range.isEndKeyInclusive());
      getSource().seek(range, columnFamilies, inclusive);
    } else {
      range = new Range(key, true, range.getEndKey(), range.isEndKeyInclusive());
      getSource().seek(range, columnFamilies, inclusive);
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    this.numVersions = 0;

    String maxVerString = options.get("maxVersions");
    if (maxVerString != null)
      this.maxVersions = Integer.parseInt(maxVerString);
    else
      this.maxVersions = 1;

    if (maxVersions < 1)
      throw new IllegalArgumentException("maxVersions for versioning iterator must be >= 1");
  }

  @Override
  public IteratorOptions describeOptions() {
    return new IteratorOptions("vers", "The VersioningIterator keeps a fixed number of versions for each key", Collections.singletonMap("maxVersions",
        "number of versions to keep for a particular key (with differing timestamps)"), null);
  }

  private static final String MAXVERSIONS_OPT = "maxVersions";

  @Override
  public boolean validateOptions(Map<String,String> options) {
    int i;
    try {
      i = Integer.parseInt(options.get(MAXVERSIONS_OPT));
    } catch (Exception e) {
      throw new IllegalArgumentException("bad integer " + MAXVERSIONS_OPT + ":" + options.get(MAXVERSIONS_OPT));
    }
    if (i < 1)
      throw new IllegalArgumentException(MAXVERSIONS_OPT + " for versioning iterator must be >= 1");
    return true;
  }

  /**
   * Encode the maximum number of versions to return onto the ScanIterator
   */
  public static void setMaxVersions(IteratorSetting cfg, int maxVersions) {
    if (maxVersions < 1)
      throw new IllegalArgumentException(MAXVERSIONS_OPT + " for versioning iterator must be >= 1");
    cfg.addOption(MAXVERSIONS_OPT, Integer.toString(maxVersions));
  }
}
