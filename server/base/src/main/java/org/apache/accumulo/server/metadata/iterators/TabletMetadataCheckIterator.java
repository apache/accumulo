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
package org.apache.accumulo.server.metadata.iterators;

import static org.apache.accumulo.core.util.LazySingletons.GSON;
import static org.apache.accumulo.server.metadata.iterators.ColumnFamilyTransformationIterator.getTabletRow;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.IteratorAdapter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadataCheck;
import org.apache.accumulo.server.metadata.ConditionalTabletMutatorImpl;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class TabletMetadataCheckIterator implements SortedKeyValueIterator<Key,Value> {

  private static final Logger log = LoggerFactory.getLogger(TabletMetadataCheckIterator.class);

  private TabletMetadataCheck check;
  private KeyExtent expectedExtent;

  private Key startKey;
  private Value topValue;
  private SortedKeyValueIterator<Key,Value> source;

  private static final String CHECK_CLASS_KEY = "checkClass";
  private static final String CHECK_DATA_KEY = "checkData";
  private static final String EXTENT_KEY = "checkExtent";
  private static final String SUCCESS = "success";

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    Preconditions.checkState(check == null && expectedExtent == null && startKey == null);
    try {
      String className = Objects.requireNonNull(options.get(CHECK_CLASS_KEY));
      String checkData = Objects.requireNonNull(options.get(CHECK_DATA_KEY));
      log.trace("Instantiating class {} using {}", className, checkData);
      Class<? extends TabletMetadataCheck> clazz =
          ClassLoaderUtil.loadClass(null, className, TabletMetadataCheck.class);
      check = GSON.get().fromJson(checkData, clazz);
      expectedExtent = KeyExtent.fromBase64(options.get(EXTENT_KEY));
      this.source = source;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    Preconditions.checkState(check != null && expectedExtent != null);

    this.startKey = null;

    Text tabletRow = getTabletRow(range);

    var expectedMetaRow = expectedExtent.toMetaRow();
    Preconditions.checkState(tabletRow.equals(expectedMetaRow), "Tablet row mismatch %s %s",
        tabletRow, expectedMetaRow);

    var colsToRead = check.columnsToRead();

    source.seek(new Range(tabletRow), colsToRead.getFamilies(),
        !colsToRead.getFamilies().isEmpty());

    if (source.hasTop()) {
      var tabletMetadata = TabletMetadata.convertRow(new IteratorAdapter(source),
          EnumSet.copyOf(colsToRead.getColumns()), false, false);

      // TODO checking the prev end row here is redundant w/ other checks that ample currently
      // does.. however we could try to make all checks eventually use this class
      if (tabletMetadata.getExtent().equals(expectedExtent)) {
        if (check.canUpdate(tabletMetadata)) {
          topValue = new Value(SUCCESS);
        } else {
          topValue = null;
        }
        log.trace("Checked tablet {} using {} {} and it was a {}", expectedExtent, check,
            tabletMetadata, topValue != null ? SUCCESS : "FAILURE");
      } else {
        topValue = null;
        log.trace(
            "Attempted to check tablet {} using {} but found another extent {}, so failing check",
            expectedExtent, check, tabletMetadata.getExtent());
      }
    } else {
      topValue = null;
      log.trace("Attempted to check tablet {} using {} but it does not exists, so failing check",
          expectedExtent, check);
    }

    this.startKey = range.getStartKey();
  }

  @Override
  public Key getTopKey() {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    if (topValue == null) {
      throw new NoSuchElementException();
    }

    return startKey;
  }

  @Override
  public Value getTopValue() {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    if (topValue == null) {
      throw new NoSuchElementException();
    }
    return topValue;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isRunningLowOnMemory() {
    return source.isRunningLowOnMemory();
  }

  @Override
  public boolean hasTop() {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    return topValue != null;
  }

  @Override
  public void next() throws IOException {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    topValue = null;
  }

  public static Condition createCondition(TabletMetadataCheck tmCheck, KeyExtent expectedExtent) {
    Objects.requireNonNull(tmCheck);
    IteratorSetting is = new IteratorSetting(ConditionalTabletMutatorImpl.INITIAL_ITERATOR_PRIO,
        TabletMetadataCheckIterator.class);
    is.addOption(CHECK_CLASS_KEY, tmCheck.getClass().getName());
    is.addOption(CHECK_DATA_KEY, GSON.get().toJson(tmCheck));
    is.addOption(EXTENT_KEY, expectedExtent.toBase64());
    return new Condition("", "").setValue(SUCCESS).setIterators(is);
  }
}
