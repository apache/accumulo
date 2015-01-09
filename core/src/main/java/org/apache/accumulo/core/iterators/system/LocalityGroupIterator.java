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
package org.apache.accumulo.core.iterators.system;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 */
public class LocalityGroupIterator extends HeapIterator implements InterruptibleIterator {

  private static final Collection<ByteSequence> EMPTY_CF_SET = Collections.emptySet();

  public static class LocalityGroup {
    private LocalityGroup(LocalityGroup localityGroup, IteratorEnvironment env) {
      this(localityGroup.columnFamilies, localityGroup.isDefaultLocalityGroup);
      this.iterator = (InterruptibleIterator) localityGroup.iterator.deepCopy(env);
    }

    public LocalityGroup(InterruptibleIterator iterator, Map<ByteSequence,MutableLong> columnFamilies, boolean isDefaultLocalityGroup) {
      this(columnFamilies, isDefaultLocalityGroup);
      this.iterator = iterator;
    }

    public LocalityGroup(Map<ByteSequence,MutableLong> columnFamilies, boolean isDefaultLocalityGroup) {
      this.isDefaultLocalityGroup = isDefaultLocalityGroup;
      this.columnFamilies = columnFamilies;
    }

    public InterruptibleIterator getIterator() {
      return iterator;
    }

    protected boolean isDefaultLocalityGroup;
    protected Map<ByteSequence,MutableLong> columnFamilies;
    private InterruptibleIterator iterator;
  }

  private LocalityGroup groups[];
  private Set<ByteSequence> nonDefaultColumnFamilies;
  private AtomicBoolean interruptFlag;

  public LocalityGroupIterator(LocalityGroup groups[], Set<ByteSequence> nonDefaultColumnFamilies) {
    super(groups.length);
    this.groups = groups;
    this.nonDefaultColumnFamilies = nonDefaultColumnFamilies;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }

  public static final int seek(HeapIterator hiter, LocalityGroup[] groups, Set<ByteSequence> nonDefaultColumnFamilies, Range range,
      Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    hiter.clear();

    int numLGSeeked = 0;

    Set<ByteSequence> cfSet;
    if (columnFamilies.size() > 0)
      if (columnFamilies instanceof Set<?>) {
        cfSet = (Set<ByteSequence>) columnFamilies;
      } else {
        cfSet = new HashSet<ByteSequence>();
        cfSet.addAll(columnFamilies);
      }
    else
      cfSet = Collections.emptySet();

    for (LocalityGroup lgr : groups) {
      // when include is set to true it means this locality groups contains
      // wanted column families
      boolean include = false;

      if (cfSet.size() == 0) {
        include = !inclusive;
      } else if (lgr.isDefaultLocalityGroup && lgr.columnFamilies == null) {
        // do not know what column families are in the default locality group,
        // only know what column families are not in it

        if (inclusive) {
          if (!nonDefaultColumnFamilies.containsAll(cfSet)) {
            // default LG may contain wanted and unwanted column families
            include = true;
          }// else - everything wanted is in other locality groups, so nothing to do
        } else {
          // must include, if all excluded column families are in other locality groups
          // then there are not unwanted column families in default LG
          include = true;
        }
      } else {
        /*
         * Need to consider the following cases for inclusive and exclusive (lgcf:locality group column family set, cf:column family set) lgcf and cf are
         * disjoint lgcf and cf are the same cf contains lgcf lgcf contains cf lgccf and cf intersect but neither is a subset of the other
         */

        for (Entry<ByteSequence,MutableLong> entry : lgr.columnFamilies.entrySet())
          if (entry.getValue().longValue() > 0)
            if (cfSet.contains(entry.getKey())) {
              if (inclusive)
                include = true;
            } else if (!inclusive) {
              include = true;
            }
      }

      if (include) {
        lgr.getIterator().seek(range, EMPTY_CF_SET, false);
        hiter.addSource(lgr.getIterator());
        numLGSeeked++;
      }// every column family is excluded, zero count, or not present
    }

    return numLGSeeked;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    seek(this, groups, nonDefaultColumnFamilies, range, columnFamilies, inclusive);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    LocalityGroup[] groupsCopy = new LocalityGroup[groups.length];

    for (int i = 0; i < groups.length; i++) {
      groupsCopy[i] = new LocalityGroup(groups[i], env);
      if (interruptFlag != null)
        groupsCopy[i].getIterator().setInterruptFlag(interruptFlag);
    }

    return new LocalityGroupIterator(groupsCopy, nonDefaultColumnFamilies);
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    this.interruptFlag = flag;
    for (LocalityGroup lgr : groups) {
      lgr.getIterator().setInterruptFlag(flag);
    }
  }

}
