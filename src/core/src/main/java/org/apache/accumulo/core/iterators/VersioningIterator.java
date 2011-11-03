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
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

public class VersioningIterator extends WrappingIterator implements OptionDescriber {
  
  private Key currentKey = new Key();
  private int numVersions;
  private int maxVersions;
  
  // lazyReset is here to provide support for environments that don't seek before calling hasTop() or next(), which is
  // technically wrong but not enforced. This has been used by compaction code in the past. lazyReset allows the
  // VersioningIterator to not require that its source tree support calls to next() and hasTop() before calls to seek(),
  // except when that is required of the VersioningIterator
  public boolean lazyReset = false;
  
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
    lazyReset = true;
  }
  
  @Override
  public void next() throws IOException {
    if (lazyReset)
      resetVersionCount();
    
    if (numVersions >= maxVersions) {
      skipRowColumn();
      resetVersionCount();
      return;
    }
    
    getSource().next();
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
    if (lazyReset)
      resetVersionCount();
    return super.hasTop();
  }
  
  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    // do not want to seek to the middle of a row
    Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);
    
    getSource().seek(seekRange, columnFamilies, inclusive);
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
    if (getSource().hasTop())
      currentKey.set(getSource().getTopKey());
    numVersions = 1;
    lazyReset = false;
  }
  
  private void skipRowColumn() throws IOException {
    Key keyToSkip = currentKey;
    getSource().next();
    
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
    
    lazyReset = true;
  }
  
  @Override
  public IteratorOptions describeOptions() {
    return new IteratorOptions("vers", "The VersioningIterator keeps a fixed number of versions for each key", Collections.singletonMap("maxVersions",
        "number of versions to keep for a particular key (with differing timestamps)"), null);
  }
  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    int i = Integer.parseInt(options.get("maxVersions"));
    if (i < 1)
      throw new IllegalArgumentException("maxVersions for versioning iterator must be >= 1");
    return true;
  }
}
