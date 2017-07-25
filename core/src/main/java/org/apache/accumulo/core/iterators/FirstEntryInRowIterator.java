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
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.Text;

public class FirstEntryInRowIterator extends SkippingIterator implements OptionDescriber {

  // options
  static final String NUM_SCANS_STRING_NAME = "scansBeforeSeek";

  // iterator predecessor seek options to pass through
  private Range latestRange;
  private Collection<ByteSequence> latestColumnFamilies;
  private boolean latestInclusive;

  // private fields
  private Text lastRowFound;
  private int numscans;

  /**
   * convenience method to set the option to optimize the frequency of scans vs. seeks
   */
  public static void setNumScansBeforeSeek(IteratorSetting cfg, int num) {
    cfg.addOption(NUM_SCANS_STRING_NAME, Integer.toString(num));
  }

  // this must be public for OptionsDescriber
  public FirstEntryInRowIterator() {
    super();
  }

  public FirstEntryInRowIterator(FirstEntryInRowIterator other, IteratorEnvironment env) {
    super();
    setSource(other.getSource().deepCopy(env));
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new FirstEntryInRowIterator(this, env);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    String o = options.get(NUM_SCANS_STRING_NAME);
    numscans = o == null ? 10 : Integer.parseInt(o);
  }

  // this is only ever called immediately after getting "next" entry
  @Override
  protected void consume() throws IOException {
    if (finished || lastRowFound == null)
      return;
    int count = 0;
    SortedKeyValueIterator<Key,Value> source = getSource();
    while (source.hasTop() && lastRowFound.equals(source.getTopKey().getRow())) {

      // try to efficiently jump to the next matching key
      if (count < numscans) {
        ++count;
        source.next(); // scan
      } else {
        // too many scans, just seek
        count = 0;

        // determine where to seek to, but don't go beyond the user-specified range
        Key nextKey = source.getTopKey().followingKey(PartialKey.ROW);
        if (!latestRange.afterEndKey(nextKey))
          source.seek(new Range(nextKey, true, latestRange.getEndKey(), latestRange.isEndKeyInclusive()), latestColumnFamilies, latestInclusive);
        else {
          finished = true;
          break;
        }
      }
    }
    lastRowFound = source.hasTop() ? source.getTopKey().getRow(lastRowFound) : null;
  }

  private boolean finished = true;

  @Override
  public boolean hasTop() {
    return !finished && getSource().hasTop();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    // save parameters for future internal seeks
    latestRange = range;
    latestColumnFamilies = columnFamilies;
    latestInclusive = inclusive;
    lastRowFound = null;

    Key startKey = range.getStartKey();
    Range seekRange = new Range(startKey == null ? null : new Key(startKey.getRow()), true, range.getEndKey(), range.isEndKeyInclusive());
    super.seek(seekRange, columnFamilies, inclusive);
    finished = false;

    if (getSource().hasTop()) {
      lastRowFound = getSource().getTopKey().getRow();
      if (range.beforeStartKey(getSource().getTopKey()))
        consume();
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    String name = "firstEntry";
    String desc = "Only allows iteration over the first entry per row";
    HashMap<String,String> namedOptions = new HashMap<>();
    namedOptions.put(NUM_SCANS_STRING_NAME, "Number of scans to try before seeking [10]");
    return new IteratorOptions(name, desc, namedOptions, null);
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    String o = options.get(NUM_SCANS_STRING_NAME);
    if (o != null && !NumberUtils.isNumber(o))
      throw new IllegalArgumentException("bad integer " + NUM_SCANS_STRING_NAME + ":" + options.get(NUM_SCANS_STRING_NAME));
    return true;
  }

}
