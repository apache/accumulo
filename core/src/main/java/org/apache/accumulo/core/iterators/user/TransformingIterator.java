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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.collections.BufferOverflowException;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The TransformingIterator allows portions of a key (except for the row) to be transformed. This iterator handles the details that come with modifying keys
 * (i.e., that the sort order could change). In order to do so, however, the iterator must put all keys sharing the same prefix in memory. Prefix is defined as
 * the parts of the key that are not modified by this iterator. That is, if the iterator modifies column qualifier and timestamp, then the prefix is row and
 * column family. In that case, the iterator must load all column qualifiers for each row/column family pair into memory. Given this constraint, care must be
 * taken by users of this iterator to ensure it is not run in such a way that will overrun memory in a tablet server.
 * <p>
 * If the implementing iterator is transforming column families, then it must also override {@code untransformColumnFamilies(Collection)} to handle the case
 * when column families are fetched at scan time. The fetched column families will/must be in the transformed space, and the untransformed column families need
 * to be passed to this iterator's source. If it is not possible to write a reverse transformation (e.g., the column family transformation depends on the row
 * value or something like that), then the iterator must not fetch specific column families (or only fetch column families that are known to not transform at
 * all).
 * <p>
 * If the implementing iterator is transforming column visibilities, then users must be careful NOT to fetch column qualifiers from the scanner. The reason for
 * this is due to ACCUMULO-??? (insert issue number).
 * <p>
 * If the implementing iterator is transforming column visibilities, then the user should be sure to supply authorizations via the {@link #AUTH_OPT} iterator
 * option (note that this is only necessary for scan scope iterators). The supplied authorizations should be in the transformed space, but the authorizations
 * supplied to the scanner should be in the untransformed space. That is, if the iterator transforms A to 1, B to 2, C to 3, etc, then the auths supplied when
 * the scanner is constructed should be A,B,C,... and the auths supplied to the iterator should be 1,2,3,... The reason for this is that the scanner performs
 * security filtering before this iterator is called, so the authorizations need to be in the original untransformed space. Since the iterator can transform
 * visibilities, it is possible that it could produce visibilities that the user cannot see, so the transformed keys must be tested to ensure the user is
 * allowed to view them. Note that this test is not necessary when the iterator is not used in the scan scope since no security filtering is performed during
 * major and minor compactions. It should also be noted that this iterator implements the security filtering rather than relying on a follow-on iterator to do
 * it so that we ensure the test is performed.
 */
abstract public class TransformingIterator extends WrappingIterator implements OptionDescriber {
  public static final String AUTH_OPT = "authorizations";
  public static final String MAX_BUFFER_SIZE_OPT = "maxBufferSize";
  private static final long DEFAULT_MAX_BUFFER_SIZE = 10000000;

  protected Logger log = LoggerFactory.getLogger(getClass());

  protected ArrayList<Pair<Key,Value>> keys = new ArrayList<>();
  protected int keyPos = -1;
  protected boolean scanning;
  protected Range seekRange;
  protected Collection<ByteSequence> seekColumnFamilies;
  protected boolean seekColumnFamiliesInclusive;

  private VisibilityEvaluator ve = null;
  private LRUMap visibleCache = null;
  private LRUMap parsedVisibilitiesCache = null;
  private long maxBufferSize;

  private static Comparator<Pair<Key,Value>> keyComparator = new Comparator<Pair<Key,Value>>() {
    @Override
    public int compare(Pair<Key,Value> o1, Pair<Key,Value> o2) {
      return o1.getFirst().compareTo(o2.getFirst());
    }
  };

  public TransformingIterator() {}

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    scanning = IteratorScope.scan.equals(env.getIteratorScope());
    if (scanning) {
      String auths = options.get(AUTH_OPT);
      if (auths != null && !auths.isEmpty()) {
        ve = new VisibilityEvaluator(new Authorizations(auths.getBytes(UTF_8)));
        visibleCache = new LRUMap(100);
      }
    }

    if (options.containsKey(MAX_BUFFER_SIZE_OPT)) {
      maxBufferSize = ConfigurationTypeHelper.getFixedMemoryAsBytes(options.get(MAX_BUFFER_SIZE_OPT));
    } else {
      maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
    }

    parsedVisibilitiesCache = new LRUMap(100);
  }

  @Override
  public IteratorOptions describeOptions() {
    String desc = "This iterator allows ranges of key to be transformed (with the exception of row transformations).";
    String authDesc = "Comma-separated list of user's scan authorizations.  "
        + "If excluded or empty, then no visibility check is performed on transformed keys.";
    String bufferDesc = "Maximum buffer size (in accumulo memory spec) to use for buffering keys before throwing a BufferOverflowException.  "
        + "Users should keep this limit in mind when deciding what to transform.  That is, if transforming the column family for example, then all "
        + "keys sharing the same row and column family must fit within this limit (along with their associated values)";
    HashMap<String,String> namedOptions = new HashMap<>();
    namedOptions.put(AUTH_OPT, authDesc);
    namedOptions.put(MAX_BUFFER_SIZE_OPT, bufferDesc);
    return new IteratorOptions(getClass().getSimpleName(), desc, namedOptions, null);
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {

    for (Entry<String,String> option : options.entrySet()) {
      try {
        if (option.getKey().equals(AUTH_OPT)) {
          new Authorizations(option.getValue().getBytes(UTF_8));
        } else if (option.getKey().equals(MAX_BUFFER_SIZE_OPT)) {
          ConfigurationTypeHelper.getFixedMemoryAsBytes(option.getValue());
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to parse opt " + option.getKey() + " " + option.getValue(), e);
      }
    }

    return true;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    TransformingIterator copy;

    try {
      copy = getClass().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    copy.setSource(getSource().deepCopy(env));

    copy.scanning = scanning;
    copy.keyPos = keyPos;
    copy.keys.addAll(keys);
    copy.seekRange = (seekRange == null) ? null : new Range(seekRange);
    copy.seekColumnFamilies = (seekColumnFamilies == null) ? null : new HashSet<>(seekColumnFamilies);
    copy.seekColumnFamiliesInclusive = seekColumnFamiliesInclusive;

    copy.ve = ve;
    if (visibleCache != null) {
      copy.visibleCache = new LRUMap(visibleCache.maxSize());
      copy.visibleCache.putAll(visibleCache);
    }

    if (parsedVisibilitiesCache != null) {
      copy.parsedVisibilitiesCache = new LRUMap(parsedVisibilitiesCache.maxSize());
      copy.parsedVisibilitiesCache.putAll(parsedVisibilitiesCache);
    }

    copy.maxBufferSize = maxBufferSize;

    return copy;
  }

  @Override
  public boolean hasTop() {
    return keyPos >= 0 && keyPos < keys.size();
  }

  @Override
  public Key getTopKey() {
    return hasTop() ? keys.get(keyPos).getFirst() : null;
  }

  @Override
  public Value getTopValue() {
    return hasTop() ? keys.get(keyPos).getSecond() : null;
  }

  @Override
  public void next() throws IOException {
    // Move on to the next entry since we returned the entry at keyPos before
    if (keyPos >= 0)
      keyPos++;

    // If we emptied out the transformed key map then transform the next key
    // set from the source. It’s possible that transformation could produce keys
    // that are outside of our range or are not visible to the end user, so after the
    // call below we might not have added any keys to the map. Keep going until
    // we either get some keys in the map or exhaust the source iterator.
    while (!hasTop() && super.hasTop())
      transformKeys();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    seekRange = new Range(range);
    seekColumnFamilies = columnFamilies;
    seekColumnFamiliesInclusive = inclusive;

    // Seek the source iterator, but use a recalculated range that ensures
    // we see all keys with the same "prefix." We need to do this since
    // transforming could change the sort order and transformed keys that
    // are before the range start could be inside the range after transformation.
    super.seek(computeReseekRange(range), untransformColumnFamilies(columnFamilies), inclusive);

    // Range clipping could cause us to trim out all the keys we transformed.
    // Keep looping until we either have some keys in the output range, or have
    // exhausted the source iterator.
    keyPos = -1; // “Clear” list so hasTop returns false to get us into the loop (transformKeys actually clears)
    while (!hasTop() && super.hasTop()) {
      // Build up a sorted list of all keys for the same prefix. When
      // people ask for keys, return from this list first until it is empty
      // before incrementing the source iterator.
      transformKeys();
    }
  }

  private static class RangeIterator implements SortedKeyValueIterator<Key,Value> {

    private SortedKeyValueIterator<Key,Value> source;
    private Key prefixKey;
    private PartialKey keyPrefix;
    private boolean hasTop = false;

    RangeIterator(SortedKeyValueIterator<Key,Value> source, Key prefixKey, PartialKey keyPrefix) {
      this.source = source;
      this.prefixKey = prefixKey;
      this.keyPrefix = keyPrefix;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasTop() {
      // only have a top if the prefix matches
      return hasTop = source.hasTop() && source.getTopKey().equals(prefixKey, keyPrefix);
    }

    @Override
    public void next() throws IOException {
      // do not let user advance too far and try to avoid reexecuting hasTop()
      if (!hasTop && !hasTop())
        throw new NoSuchElementException();
      hasTop = false;
      source.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Key getTopKey() {
      return source.getTopKey();
    }

    @Override
    public Value getTopValue() {
      return source.getTopValue();
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      throw new UnsupportedOperationException();
    }

  }

  /**
   * Reads all keys matching the first key's prefix from the source iterator, transforms them, and sorts the resulting keys. Transformed keys that fall outside
   * of our seek range or can't be seen by the user are excluded.
   */
  protected void transformKeys() throws IOException {
    keyPos = -1;
    keys.clear();
    final Key prefixKey = super.hasTop() ? new Key(super.getTopKey()) : null;

    transformRange(new RangeIterator(getSource(), prefixKey, getKeyPrefix()), new KVBuffer() {

      long appened = 0;

      @Override
      public void append(Key key, Value val) {
        // ensure the key provided by the user has the correct prefix
        if (!key.equals(prefixKey, getKeyPrefix()))
          throw new IllegalArgumentException("Key prefixes are not equal " + key + " " + prefixKey);

        // Transformation could have produced a key that falls outside
        // of the seek range, or one that the user cannot see. Check
        // these before adding it to the output list.
        if (includeTransformedKey(key)) {

          // try to defend against a scan or compaction using all memory in a tablet server
          if (appened > maxBufferSize)
            throw new BufferOverflowException("Exceeded buffer size of " + maxBufferSize + ", prefixKey: " + prefixKey);

          if (getSource().hasTop() && key == getSource().getTopKey())
            key = new Key(key);
          keys.add(new Pair<>(key, new Value(val)));
          appened += (key.getSize() + val.getSize() + 128);
        }
      }
    });

    // consume any key in range that user did not consume
    while (super.hasTop() && super.getTopKey().equals(prefixKey, getKeyPrefix())) {
      super.next();
    }

    if (!keys.isEmpty()) {
      Collections.sort(keys, keyComparator);
      keyPos = 0;
    }
  }

  /**
   * Determines whether or not to include {@code transformedKey} in the output. It is possible that transformation could have produced a key that falls outside
   * of the seek range, a key with a visibility the user can't see, a key with a visibility that doesn't parse, or a key with a column family that wasn't
   * fetched. We only do some checks (outside the range, user can see) if we're scanning. The range check is not done for major/minor compaction since seek
   * ranges won't be in our transformed key space and we will never change the row so we can't produce keys that would fall outside the tablet anyway.
   *
   * @param transformedKey
   *          the key to check
   * @return {@code true} if the key should be included and {@code false} if not
   */
  protected boolean includeTransformedKey(Key transformedKey) {
    boolean include = canSee(transformedKey);
    if (scanning && seekRange != null) {
      include = include && seekRange.contains(transformedKey);
    }
    return include;
  }

  /**
   * Indicates whether or not the user is able to see {@code key}. If the user has not supplied authorizations, or the iterator is not in the scan scope, then
   * this method simply returns {@code true}. Otherwise, {@code key}'s column visibility is tested against the user-supplied authorizations, and the test result
   * is returned. For performance, the test results are cached so that the same visibility is not tested multiple times.
   *
   * @param key
   *          the key to test
   * @return {@code true} if the key is visible or iterator is not scanning, and {@code false} if not
   */
  protected boolean canSee(Key key) {
    // Ensure that the visibility (which could have been transformed) parses. Must always do this check, even if visibility is not evaluated.
    ByteSequence visibility = key.getColumnVisibilityData();
    ColumnVisibility colVis = null;
    Boolean parsed = (Boolean) parsedVisibilitiesCache.get(visibility);
    if (parsed == null) {
      try {
        colVis = new ColumnVisibility(visibility.toArray());
        parsedVisibilitiesCache.put(visibility, Boolean.TRUE);
      } catch (BadArgumentException e) {
        log.error("Parse error after transformation : " + visibility);
        parsedVisibilitiesCache.put(visibility, Boolean.FALSE);
        if (scanning) {
          return false;
        } else {
          throw e;
        }
      }
    } else if (!parsed) {
      if (scanning)
        return false;
      else
        throw new IllegalStateException();
    }

    Boolean visible = canSeeColumnFamily(key);

    if (!scanning || !visible || ve == null || visibleCache == null || visibility.length() == 0)
      return visible;

    visible = (Boolean) visibleCache.get(visibility);
    if (visible == null) {
      try {
        if (colVis == null)
          colVis = new ColumnVisibility(visibility.toArray());
        visible = ve.evaluate(colVis);
        visibleCache.put(visibility, visible);
      } catch (VisibilityParseException e) {
        log.error("Parse Error", e);
        visible = Boolean.FALSE;
      } catch (BadArgumentException e) {
        log.error("Parse Error", e);
        visible = Boolean.FALSE;
      }
    }

    return visible;
  }

  /**
   * Indicates whether or not {@code key} can be seen, according to the fetched column families for this iterator.
   *
   * @param key
   *          the key whose column family is to be tested
   * @return {@code true} if {@code key}'s column family is one of those fetched in the set passed to our {@link #seek(Range, Collection, boolean)} method
   */
  protected boolean canSeeColumnFamily(Key key) {
    boolean visible = true;
    if (seekColumnFamilies != null) {
      ByteSequence columnFamily = key.getColumnFamilyData();
      if (seekColumnFamiliesInclusive)
        visible = seekColumnFamilies.contains(columnFamily);
      else
        visible = !seekColumnFamilies.contains(columnFamily);
    }
    return visible;
  }

  /**
   * Possibly expand {@code range} to include everything for the key prefix we are working with. That is, if our prefix is ROW_COLFAM, then we need to expand
   * the range so we're sure to include all entries having the same row and column family as the start/end of the range.
   *
   * @param range
   *          the range to expand
   * @return the modified range
   */
  protected Range computeReseekRange(Range range) {
    Key startKey = range.getStartKey();
    boolean startKeyInclusive = range.isStartKeyInclusive();
    // If anything after the prefix is set, then clip the key so we include
    // everything for the prefix.
    if (isSetAfterPart(startKey, getKeyPrefix())) {
      startKey = copyPartialKey(startKey, getKeyPrefix());
      startKeyInclusive = true;
    }
    Key endKey = range.getEndKey();
    boolean endKeyInclusive = range.isEndKeyInclusive();
    if (isSetAfterPart(endKey, getKeyPrefix())) {
      endKey = endKey.followingKey(getKeyPrefix());
      endKeyInclusive = true;
    }
    return new Range(startKey, startKeyInclusive, endKey, endKeyInclusive);
  }

  /**
   * Indicates whether or not any part of {@code key} excluding {@code part} is set. For example, if part is ROW_COLFAM_COLQUAL, then this method determines
   * whether or not the column visibility, timestamp, or delete flag is set on {@code key}.
   *
   * @param key
   *          the key to check
   * @param part
   *          the part of the key that doesn't need to be checked (everything after does)
   * @return {@code true} if anything after {@code part} is set on {@code key}, and {@code false} if not
   */
  protected boolean isSetAfterPart(Key key, PartialKey part) {
    if (key != null) {
      switch (part) {
        case ROW:
          return key.getColumnFamilyData().length() > 0 || key.getColumnQualifierData().length() > 0 || key.getColumnVisibilityData().length() > 0
              || key.getTimestamp() < Long.MAX_VALUE || key.isDeleted();
        case ROW_COLFAM:
          return key.getColumnQualifierData().length() > 0 || key.getColumnVisibilityData().length() > 0 || key.getTimestamp() < Long.MAX_VALUE
              || key.isDeleted();
        case ROW_COLFAM_COLQUAL:
          return key.getColumnVisibilityData().length() > 0 || key.getTimestamp() < Long.MAX_VALUE || key.isDeleted();
        case ROW_COLFAM_COLQUAL_COLVIS:
          return key.getTimestamp() < Long.MAX_VALUE || key.isDeleted();
        case ROW_COLFAM_COLQUAL_COLVIS_TIME:
          return key.isDeleted();
        case ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL:
          return false;
      }
    }
    return false;
  }

  /**
   * Creates a copy of {@code key}, copying only the parts of the key specified in {@code part}. For example, if {@code part} is ROW_COLFAM_COLQUAL, then this
   * method would copy the row, column family, and column qualifier from {@code key} into a new key.
   *
   * @param key
   *          the key to copy
   * @param part
   *          the parts of {@code key} to copy
   * @return the new key containing {@code part} of {@code key}
   */
  protected Key copyPartialKey(Key key, PartialKey part) {
    Key keyCopy;
    switch (part) {
      case ROW:
        keyCopy = new Key(key.getRow());
        break;
      case ROW_COLFAM:
        keyCopy = new Key(key.getRow(), key.getColumnFamily());
        break;
      case ROW_COLFAM_COLQUAL:
        keyCopy = new Key(key.getRow(), key.getColumnFamily(), key.getColumnQualifier());
        break;
      case ROW_COLFAM_COLQUAL_COLVIS:
        keyCopy = new Key(key.getRow(), key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibility());
        break;
      case ROW_COLFAM_COLQUAL_COLVIS_TIME:
        keyCopy = new Key(key.getRow(), key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibility(), key.getTimestamp());
        break;
      default:
        throw new IllegalArgumentException("Unsupported key part: " + part);
    }
    return keyCopy;
  }

  /**
   * Make a new key with all parts (including delete flag) coming from {@code originalKey} but use {@code newColFam} as the column family.
   */
  protected Key replaceColumnFamily(Key originalKey, Text newColFam) {
    byte[] row = originalKey.getRowData().toArray();
    byte[] cf = newColFam.getBytes();
    byte[] cq = originalKey.getColumnQualifierData().toArray();
    byte[] cv = originalKey.getColumnVisibilityData().toArray();
    long timestamp = originalKey.getTimestamp();
    Key newKey = new Key(row, 0, row.length, cf, 0, newColFam.getLength(), cq, 0, cq.length, cv, 0, cv.length, timestamp);
    newKey.setDeleted(originalKey.isDeleted());
    return newKey;
  }

  /**
   * Make a new key with all parts (including delete flag) coming from {@code originalKey} but use {@code newColQual} as the column qualifier.
   */
  protected Key replaceColumnQualifier(Key originalKey, Text newColQual) {
    byte[] row = originalKey.getRowData().toArray();
    byte[] cf = originalKey.getColumnFamilyData().toArray();
    byte[] cq = newColQual.getBytes();
    byte[] cv = originalKey.getColumnVisibilityData().toArray();
    long timestamp = originalKey.getTimestamp();
    Key newKey = new Key(row, 0, row.length, cf, 0, cf.length, cq, 0, newColQual.getLength(), cv, 0, cv.length, timestamp);
    newKey.setDeleted(originalKey.isDeleted());
    return newKey;
  }

  /**
   * Make a new key with all parts (including delete flag) coming from {@code originalKey} but use {@code newColVis} as the column visibility.
   */
  protected Key replaceColumnVisibility(Key originalKey, Text newColVis) {
    byte[] row = originalKey.getRowData().toArray();
    byte[] cf = originalKey.getColumnFamilyData().toArray();
    byte[] cq = originalKey.getColumnQualifierData().toArray();
    byte[] cv = newColVis.getBytes();
    long timestamp = originalKey.getTimestamp();
    Key newKey = new Key(row, 0, row.length, cf, 0, cf.length, cq, 0, cq.length, cv, 0, newColVis.getLength(), timestamp);
    newKey.setDeleted(originalKey.isDeleted());
    return newKey;
  }

  /**
   * Make a new key with a column family, column qualifier, and column visibility. Copy the rest of the parts of the key (including delete flag) from
   * {@code originalKey}.
   */
  protected Key replaceKeyParts(Key originalKey, Text newColFam, Text newColQual, Text newColVis) {
    byte[] row = originalKey.getRowData().toArray();
    byte[] cf = newColFam.getBytes();
    byte[] cq = newColQual.getBytes();
    byte[] cv = newColVis.getBytes();
    long timestamp = originalKey.getTimestamp();
    Key newKey = new Key(row, 0, row.length, cf, 0, newColFam.getLength(), cq, 0, newColQual.getLength(), cv, 0, newColVis.getLength(), timestamp);
    newKey.setDeleted(originalKey.isDeleted());
    return newKey;
  }

  /**
   * Make a new key with a column qualifier, and column visibility. Copy the rest of the parts of the key (including delete flag) from {@code originalKey}.
   */
  protected Key replaceKeyParts(Key originalKey, Text newColQual, Text newColVis) {
    byte[] row = originalKey.getRowData().toArray();
    byte[] cf = originalKey.getColumnFamilyData().toArray();
    byte[] cq = newColQual.getBytes();
    byte[] cv = newColVis.getBytes();
    long timestamp = originalKey.getTimestamp();
    Key newKey = new Key(row, 0, row.length, cf, 0, cf.length, cq, 0, newColQual.getLength(), cv, 0, newColVis.getLength(), timestamp);
    newKey.setDeleted(originalKey.isDeleted());
    return newKey;
  }

  /**
   * Reverses the transformation applied to column families that are fetched at seek time. If this iterator is transforming column families, then this method
   * should be overridden to reverse the transformation on the supplied collection of column families. This is necessary since the fetch/seek will be performed
   * in the transformed space, but when passing the column family set on to the source, the column families need to be in the untransformed space.
   *
   * @param columnFamilies
   *          the column families that have been fetched at seek time
   * @return the untransformed column families that would transform info {@code columnFamilies}
   */
  protected Collection<ByteSequence> untransformColumnFamilies(Collection<ByteSequence> columnFamilies) {
    return columnFamilies;
  }

  /**
   * Indicates the prefix of keys that will be transformed by this iterator. In other words, this is the part of the key that will <i>not</i> be transformed by
   * this iterator. For example, if this method returns ROW_COLFAM, then {@link #transformKeys()} may be changing the column qualifier, column visibility, or
   * timestamp, but it won't be changing the row or column family.
   *
   * @return the part of the key this iterator is not transforming
   */
  abstract protected PartialKey getKeyPrefix();

  public interface KVBuffer {
    void append(Key key, Value val);
  }

  /**
   * Transforms {@code input}. This method must not change the row part of the key, and must only change the parts of the key after the return value of
   * {@link #getKeyPrefix()}. Implementors must also remember to copy the delete flag from {@code originalKey} onto the new key. Or, implementors should use one
   * of the helper methods to produce the new key. See any of the replaceKeyParts methods.
   *
   * @param input
   *          An iterator over a group of keys with the same prefix. This iterator provides an efficient view, bounded by the prefix, of the underlying iterator
   *          and can not be seeked.
   * @param output
   *          An output buffer that holds transformed key values. All key values added to the buffer must have the same prefix as the input keys.
   * @see #replaceColumnFamily(Key, Text)
   * @see #replaceColumnQualifier(Key, Text)
   * @see #replaceColumnVisibility(Key, Text)
   * @see #replaceKeyParts(Key, Text, Text)
   * @see #replaceKeyParts(Key, Text, Text, Text)
   */
  abstract protected void transformRange(SortedKeyValueIterator<Key,Value> input, KVBuffer output) throws IOException;

  /**
   * Configure authorizations used for post transformation filtering.
   *
   */
  public static void setAuthorizations(IteratorSetting config, Authorizations auths) {
    config.addOption(AUTH_OPT, auths.serialize());
  }

  /**
   * Configure the maximum amount of memory that can be used for transformation. If this memory is exceeded an exception will be thrown.
   *
   * @param maxBufferSize
   *          size in bytes
   */
  public static void setMaxBufferSize(IteratorSetting config, long maxBufferSize) {
    config.addOption(MAX_BUFFER_SIZE_OPT, maxBufferSize + "");
  }

}
