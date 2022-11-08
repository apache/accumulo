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
package org.apache.accumulo.core.metadata.schema;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Iterator;
import java.util.function.Function;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

/**
 * Tablets for a table in the metadata table should form a linked list. This iterator detects when
 * tablets do not form a linked list and backs up when this happens.
 *
 * <p>
 * The purpose of this is to hide inconsistencies caused by splits and detect anomalies in the
 * metadata table.
 *
 * <p>
 * If a tablet that was returned by this iterator is subsequently deleted from the metadata table,
 * then this iterator will throw a TabletDeletedException. This could occur when a table is merged.
 */
public class LinkingIterator implements Iterator<TabletMetadata> {

  private static final Logger log = LoggerFactory.getLogger(LinkingIterator.class);

  private Range range;
  private Function<Range,Iterator<TabletMetadata>> iteratorFactory;
  private Iterator<TabletMetadata> source;
  private TabletMetadata prevTablet = null;

  LinkingIterator(Function<Range,Iterator<TabletMetadata>> iteratorFactory, Range range) {
    this.range = range;
    this.iteratorFactory = iteratorFactory;
    source = iteratorFactory.apply(range);
  }

  @Override
  public boolean hasNext() {
    boolean hasNext = source.hasNext();

    // Always expect the default tablet to exist for a table. The following checks for the case when
    // the default tablet was not seen when it should have been seen.
    if (!hasNext && prevTablet != null && prevTablet.getEndRow() != null) {
      Text defaultTabletRow = TabletsSection.encodeRow(prevTablet.getTableId(), null);
      if (range.contains(new Key(defaultTabletRow))) {
        throw new IllegalStateException(
            "Scan range included default tablet, but did not see default tablet.  Last tablet seen : "
                + prevTablet.getExtent());
      }
    }

    return hasNext;
  }

  static boolean goodTransition(TabletMetadata prev, TabletMetadata curr) {
    if (!curr.sawPrevEndRow()) {
      log.warn("Tablet {} had no prev end row.", curr.getExtent());
      return false;
    }

    if (curr.getTableId().equals(prev.getTableId())) {
      if (prev.getEndRow() == null) {
        throw new IllegalStateException("Null end row for tablet in middle of table: "
            + prev.getExtent() + " " + curr.getExtent());
      }

      if (curr.getPrevEndRow() == null || !prev.getEndRow().equals(curr.getPrevEndRow())) {
        log.debug("Tablets end row and prev end row not equals {} {} ", prev.getExtent(),
            curr.getExtent());
        return false;
      }
    } else {
      if (prev.getEndRow() != null) {
        log.debug("Non-null end row for last tablet in table: " + prev.getExtent() + " "
            + curr.getExtent());
        return false;
      }

      if (curr.getPrevEndRow() != null) {
        log.debug("First tablet for table had prev end row {} {} ", prev.getExtent(),
            curr.getExtent());
        return false;
      }
    }

    return true;
  }

  private void resetSource() {
    if (prevTablet == null) {
      log.debug("Resetting scanner to {}", range);
      source = iteratorFactory.apply(range);
    } else {
      // get the metadata table row for the previous tablet
      Text prevMetaRow = TabletsSection.encodeRow(prevTablet.getTableId(), prevTablet.getEndRow());

      // ensure the previous tablet still exists in the metadata table
      if (Iterators.size(iteratorFactory.apply(new Range(prevMetaRow))) == 0) {
        throw new TabletDeletedException("Tablet " + prevMetaRow + " was deleted while iterating");
      }

      // start scanning at next possible row in metadata table
      Range seekRange = new Range(new Key(prevMetaRow).followingKey(PartialKey.ROW), true,
          range.getEndKey(), range.isEndKeyInclusive());

      log.debug("Resetting scanner to {}", seekRange);

      source = iteratorFactory.apply(seekRange);
    }
  }

  @Override
  public TabletMetadata next() {

    long sleepTime = 250;

    TabletMetadata currTablet = null;
    while (currTablet == null) {
      TabletMetadata tmp = source.next();

      if (prevTablet == null) {
        if (tmp.sawPrevEndRow()) {

          Text prevMetaRow = null;

          KeyExtent extent = tmp.getExtent();

          if (extent.prevEndRow() != null) {
            prevMetaRow = TabletsSection.encodeRow(extent.tableId(), extent.prevEndRow());
          }

          // If the first tablet seen has a prev endrow within the range it means a preceding tablet
          // exists that we need to go back and read. This could be caused by the first tablet in
          // the range splitting concurrently while this code is reading.

          if (prevMetaRow == null || range.beforeStartKey(new Key(prevMetaRow))) {
            currTablet = tmp;
          } else {
            log.debug(
                "First tablet seen provides evidence of earlier tablet in range, retrying {} {} ",
                prevMetaRow, range);
          }
        } else {
          log.warn("Tablet has no prev end row " + tmp.getTableId() + " " + tmp.getEndRow());
        }
      } else if (goodTransition(prevTablet, tmp)) {
        currTablet = tmp;
      }

      if (currTablet == null) {
        sleepUninterruptibly(sleepTime, MILLISECONDS);
        resetSource();
        sleepTime = Math.min(2 * sleepTime, 5000);
      }
    }

    prevTablet = currTablet;
    return currTablet;
  }
}
