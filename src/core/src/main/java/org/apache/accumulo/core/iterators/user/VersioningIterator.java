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
  
  private Key currentKey = new Key();
  private int numVersions;
  private int maxVersions;
  
  @Override
  public VersioningIterator deepCopy(IteratorEnvironment env) {
    return new VersioningIterator(this, env);
  }
  
  private VersioningIterator(VersioningIterator other, IteratorEnvironment env) {
    setSource(other.getSource().deepCopy(env));
    maxVersions = other.maxVersions;
  }
  
  public VersioningIterator() {}
  
  public VersioningIterator(SortedKeyValueIterator<Key,Value> iterator, int maxVersions) {
    if (maxVersions < 1)
      throw new IllegalArgumentException("maxVersions for versioning iterator must be >= 1");
    this.setSource(iterator);
    this.maxVersions = maxVersions;
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
  public boolean hasTop() {
    return super.hasTop();
  }
  
  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    // do not want to seek to the middle of a row
    Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);
    
    super.seek(seekRange, columnFamilies, inclusive);
    resetVersionCount();
    
    if (range.getStartKey() != null) {
      while (getSource().hasTop() && getSource().getTopKey().compareTo(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME) < 0) {
        // the value has a more recent time stamp, so
        // pass it up
        // log.debug("skipping "+getTopKey());
        next();
      }
      
      while (hasTop() && range.beforeStartKey(getTopKey())) {
        next();
      }
    }
  }
  
  private void resetVersionCount() {
    if (super.hasTop())
      currentKey.set(getSource().getTopKey());
    numVersions = 1;
  }
  
  private void skipRowColumn() throws IOException {
    Key keyToSkip = currentKey;
    super.next();
    
    while (getSource().hasTop() && getSource().getTopKey().equals(keyToSkip, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      getSource().next();
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
    int i = Integer.parseInt(options.get(MAXVERSIONS_OPT));
    if (i < 1)
      throw new IllegalArgumentException(MAXVERSIONS_OPT + " for versioning iterator must be >= 1");
    return true;
  }
  
  /**
   * Encode the maximum number of versions to return onto the ScanIterator
   * 
   * @param cfg
   * @param maxVersions
   */
  public static void setMaxVersions(IteratorSetting cfg, int maxVersions) {
    cfg.addOption(MAXVERSIONS_OPT, Integer.toString(maxVersions));
  }
}
