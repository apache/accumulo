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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

import com.google.common.collect.ImmutableSet;

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

  public static class LocalityGroupContext {
    final List<LocalityGroup> groups;
    final LocalityGroup defaultGroup;
    final Map<ByteSequence,LocalityGroup> groupByCf;

    public LocalityGroupContext(LocalityGroup[] groups) {
      this.groups = Collections.unmodifiableList(Arrays.asList(groups));
      this.groupByCf = new HashMap<>();
      LocalityGroup foundDefault = null;

      for (LocalityGroup group : groups) {
        if (group.isDefaultLocalityGroup && group.columnFamilies == null) {
          if (foundDefault != null) {
            throw new IllegalStateException("Found multiple default locality groups");
          }
          foundDefault = group;
        } else {
          for (Entry<ByteSequence,MutableLong> entry : group.columnFamilies.entrySet()) {
            if (entry.getValue().longValue() > 0) {
              if (groupByCf.containsKey(entry.getKey())) {
                throw new IllegalStateException("Found the same cf in multiple locality groups");
              }
              groupByCf.put(entry.getKey(), group);
            }
          }
        }
      }
      defaultGroup = foundDefault;
    }
  }

  /**
   * This will cache the arguments used in the seek call along with the locality groups seeked.
   */
  public static class LocalityGroupSeekCache {
    private ImmutableSet<ByteSequence> lastColumnFamilies;
    private volatile boolean lastInclusive;
    private Collection<LocalityGroup> lastUsed;

    public ImmutableSet<ByteSequence> getLastColumnFamilies() {
      return lastColumnFamilies;
    }

    public boolean isLastInclusive() {
      return lastInclusive;
    }

    public Collection<LocalityGroup> getLastUsed() {
      return lastUsed;
    }

    public int getNumLGSeeked() {
      return (lastUsed == null ? 0 : lastUsed.size());
    }
  }

  private final LocalityGroupContext lgContext;
  private LocalityGroupSeekCache lgCache;
  private AtomicBoolean interruptFlag;

  public LocalityGroupIterator(LocalityGroup groups[]) {
    super(groups.length);
    this.lgContext = new LocalityGroupContext(groups);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * This is the seek work horse for a HeapIterator with locality groups (uses by the InMemory and RFile mechanisms). This method will find the locality groups
   * to use in the LocalityGroupContext, and will seek those groups.
   *
   * @param hiter
   *          The heap iterator
   * @param lgContext
   *          The locality groups
   * @param range
   *          The range to seek
   * @param columnFamilies
   *          The column fams to seek
   * @param inclusive
   *          The inclusiveness of the column fams
   * @return The locality groups seeked
   * @throws IOException
   *           thrown if an locality group seek fails
   */
  static final Collection<LocalityGroup> _seek(HeapIterator hiter, LocalityGroupContext lgContext, Range range, Collection<ByteSequence> columnFamilies,
      boolean inclusive) throws IOException {
    hiter.clear();

    Set<ByteSequence> cfSet;
    if (columnFamilies.size() > 0)
      if (columnFamilies instanceof Set<?>) {
        cfSet = (Set<ByteSequence>) columnFamilies;
      } else {
        cfSet = new HashSet<>();
        cfSet.addAll(columnFamilies);
      }
    else
      cfSet = Collections.emptySet();

    // determine the set of groups to use
    Collection<LocalityGroup> groups = Collections.emptyList();

    // if no column families specified, then include all groups unless !inclusive
    if (cfSet.size() == 0) {
      if (!inclusive) {
        groups = lgContext.groups;
      }
    } else {
      groups = new HashSet<>();

      // do not know what column families are in the default locality group,
      // only know what column families are not in it
      if (lgContext.defaultGroup != null) {
        if (inclusive) {
          if (!lgContext.groupByCf.keySet().containsAll(cfSet)) {
            // default LG may contain wanted and unwanted column families
            groups.add(lgContext.defaultGroup);
          }// else - everything wanted is in other locality groups, so nothing to do
        } else {
          // must include the default group as it may include cfs not in our cfSet
          groups.add(lgContext.defaultGroup);
        }
      }

      /*
       * Need to consider the following cases for inclusive and exclusive (lgcf:locality group column family set, cf:column family set) lgcf and cf are disjoint
       * lgcf and cf are the same cf contains lgcf lgcf contains cf lgccf and cf intersect but neither is a subset of the other
       */
      if (!inclusive) {
        for (Entry<ByteSequence,LocalityGroup> entry : lgContext.groupByCf.entrySet()) {
          if (!cfSet.contains(entry.getKey())) {
            groups.add(entry.getValue());
          }
        }
      } else if (lgContext.groupByCf.size() <= cfSet.size()) {
        for (Entry<ByteSequence,LocalityGroup> entry : lgContext.groupByCf.entrySet()) {
          if (cfSet.contains(entry.getKey())) {
            groups.add(entry.getValue());
          }
        }
      } else {
        for (ByteSequence cf : cfSet) {
          LocalityGroup group = lgContext.groupByCf.get(cf);
          if (group != null) {
            groups.add(group);
          }
        }
      }
    }

    for (LocalityGroup lgr : groups) {
      lgr.getIterator().seek(range, EMPTY_CF_SET, false);
      hiter.addSource(lgr.getIterator());
    }

    return groups;
  }

  /**
   * This seek method will reuse the supplied LocalityGroupSeekCache if it can. Otherwise it will delegate to the _seek method.
   *
   * @param hiter
   *          The heap iterator
   * @param lgContext
   *          The locality groups
   * @param range
   *          The range to seek
   * @param columnFamilies
   *          The column fams to seek
   * @param inclusive
   *          The inclusiveness of the column fams
   * @param lgSeekCache
   *          A cache returned by the previous call to this method
   * @return A cache for this seek call
   * @throws IOException
   *           thrown if an locality group seek fails
   */
  public static LocalityGroupSeekCache seek(HeapIterator hiter, LocalityGroupContext lgContext, Range range, Collection<ByteSequence> columnFamilies,
      boolean inclusive, LocalityGroupSeekCache lgSeekCache) throws IOException {
    if (lgSeekCache == null)
      lgSeekCache = new LocalityGroupSeekCache();

    // determine if the arguments have changed since the last time
    boolean sameArgs = false;
    ImmutableSet<ByteSequence> cfSet = null;
    if (lgSeekCache.lastUsed != null && inclusive == lgSeekCache.lastInclusive) {
      if (columnFamilies instanceof Set) {
        sameArgs = lgSeekCache.lastColumnFamilies.equals(columnFamilies);
      } else {
        cfSet = ImmutableSet.copyOf(columnFamilies);
        sameArgs = lgSeekCache.lastColumnFamilies.equals(cfSet);
      }
    }

    // if the column families and inclusiveness have not changed, then we can simply re-seek the
    // locality groups we discovered last round and rebuild the heap.
    if (sameArgs) {
      hiter.clear();
      for (LocalityGroup lgr : lgSeekCache.lastUsed) {
        lgr.getIterator().seek(range, EMPTY_CF_SET, false);
        hiter.addSource(lgr.getIterator());
      }
    } else { // otherwise capture the parameters, and use the static seek method to locate the locality groups to use.
      lgSeekCache.lastColumnFamilies = (cfSet == null ? ImmutableSet.copyOf(columnFamilies) : cfSet);
      lgSeekCache.lastInclusive = inclusive;
      lgSeekCache.lastUsed = _seek(hiter, lgContext, range, columnFamilies, inclusive);
    }

    return lgSeekCache;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    lgCache = seek(this, lgContext, range, columnFamilies, inclusive, lgCache);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    LocalityGroup[] groupsCopy = new LocalityGroup[lgContext.groups.size()];

    for (int i = 0; i < lgContext.groups.size(); i++) {
      groupsCopy[i] = new LocalityGroup(lgContext.groups.get(i), env);
      if (interruptFlag != null)
        groupsCopy[i].getIterator().setInterruptFlag(interruptFlag);
    }

    return new LocalityGroupIterator(groupsCopy);
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    this.interruptFlag = flag;
    for (LocalityGroup lgr : lgContext.groups) {
      lgr.getIterator().setInterruptFlag(flag);
    }
  }

}
