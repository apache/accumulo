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
package org.apache.accumulo.server.util;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

/**
 * This class iterates over the metadata table returning all key values for a tablet in one chunk. As it scans the metadata table it checks the correctness of
 * the metadata table, and rescans if needed. So the tablet key/values returned by this iterator should satisfy the sorted linked list property of the metadata
 * table.
 *
 * The purpose of this is to hide inconsistencies caused by splits and detect anomalies in the metadata table.
 *
 * If a tablet that was returned by this iterator is subsequently deleted from the metadata table, then this iterator will throw a TabletDeletedException. This
 * could occur when a table is merged.
 */
public class TabletIterator implements Iterator<Map<Key,Value>> {

  private static final Logger log = LoggerFactory.getLogger(TabletIterator.class);

  private SortedMap<Key,Value> currentTabletKeys;

  private Text lastTablet;

  private Scanner scanner;
  private Iterator<Entry<Key,Value>> iter;

  private boolean returnPrevEndRow;

  private boolean returnDir;

  private Range range;

  public static class TabletDeletedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TabletDeletedException(String msg) {
      super(msg);
    }
  }

  /**
   *
   * @param s
   *          A scanner over the entire metadata table configure to fetch needed columns.
   */
  public TabletIterator(Scanner s, Range range, boolean returnPrevEndRow, boolean returnDir) {
    this.scanner = s;
    this.range = range;
    this.scanner.setRange(range);
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
    this.iter = s.iterator();
    this.returnPrevEndRow = returnPrevEndRow;
    this.returnDir = returnDir;
  }

  @Override
  public boolean hasNext() {
    while (currentTabletKeys == null) {

      currentTabletKeys = scanToPrevEndRow();
      if (currentTabletKeys.size() == 0) {
        break;
      }

      Key prevEndRowKey = currentTabletKeys.lastKey();
      Value prevEndRowValue = currentTabletKeys.get(prevEndRowKey);

      if (!TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(prevEndRowKey)) {
        log.debug("{}", currentTabletKeys);
        throw new RuntimeException("Unexpected key " + prevEndRowKey);
      }

      Text per = KeyExtent.decodePrevEndRow(prevEndRowValue);
      Text lastEndRow;

      if (lastTablet == null) {
        lastEndRow = null;
      } else {
        lastEndRow = new KeyExtent(lastTablet, (Text) null).getEndRow();

        // do table transition sanity check
        Table.ID lastTable = new KeyExtent(lastTablet, (Text) null).getTableId();
        Table.ID currentTable = new KeyExtent(prevEndRowKey.getRow(), (Text) null).getTableId();

        if (!lastTable.equals(currentTable) && (per != null || lastEndRow != null)) {
          log.info("Metadata inconsistency on table transition : {} {} {} {}", lastTable, currentTable, per, lastEndRow);

          currentTabletKeys = null;
          resetScanner();

          sleepUninterruptibly(250, TimeUnit.MILLISECONDS);

          continue;
        }
      }

      boolean perEqual = (per == null && lastEndRow == null) || (per != null && lastEndRow != null && per.equals(lastEndRow));

      if (!perEqual) {

        log.info("Metadata inconsistency : {} != {} metadataKey = {}", per, lastEndRow, prevEndRowKey);

        currentTabletKeys = null;
        resetScanner();

        sleepUninterruptibly(250, TimeUnit.MILLISECONDS);

        continue;

      }
      // this tablet is good, so set it as the last tablet
      lastTablet = prevEndRowKey.getRow();
    }

    return currentTabletKeys.size() > 0;
  }

  @Override
  public Map<Key,Value> next() {

    if (!hasNext())
      throw new NoSuchElementException();

    Map<Key,Value> tmp = currentTabletKeys;
    currentTabletKeys = null;

    Set<Entry<Key,Value>> es = tmp.entrySet();
    Iterator<Entry<Key,Value>> esIter = es.iterator();

    while (esIter.hasNext()) {
      Map.Entry<Key,Value> entry = esIter.next();
      if (!returnPrevEndRow && TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(entry.getKey())) {
        esIter.remove();
      }

      if (!returnDir && TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(entry.getKey())) {
        esIter.remove();
      }
    }

    return tmp;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private SortedMap<Key,Value> scanToPrevEndRow() {

    Text curMetaDataRow = null;

    TreeMap<Key,Value> tm = new TreeMap<>();

    boolean sawPrevEndRow = false;

    while (true) {
      while (iter.hasNext()) {
        Entry<Key,Value> entry = iter.next();

        if (curMetaDataRow == null) {
          curMetaDataRow = entry.getKey().getRow();
        }

        if (!curMetaDataRow.equals(entry.getKey().getRow())) {
          // tablet must not have a prev end row, try scanning again
          break;
        }

        tm.put(entry.getKey(), entry.getValue());

        if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(entry.getKey())) {
          sawPrevEndRow = true;
          break;
        }
      }

      if (!sawPrevEndRow && tm.size() > 0) {
        log.warn("Metadata problem : tablet {} has no prev end row", curMetaDataRow);
        resetScanner();
        curMetaDataRow = null;
        tm.clear();
        sleepUninterruptibly(250, TimeUnit.MILLISECONDS);
      } else {
        break;
      }
    }

    return tm;
  }

  protected void resetScanner() {

    Range range;

    if (lastTablet == null) {
      range = this.range;
    } else {
      // check to see if the last tablet still exist
      range = new Range(lastTablet, true, lastTablet, true);
      scanner.setRange(range);
      int count = Iterators.size(scanner.iterator());

      if (count == 0)
        throw new TabletDeletedException("Tablet " + lastTablet + " was deleted while iterating");

      // start right after the last good tablet
      range = new Range(new Key(lastTablet).followingKey(PartialKey.ROW), true, this.range.getEndKey(), this.range.isEndKeyInclusive());
    }

    log.info("Resetting {} scanner to {}", MetadataTable.NAME, range);

    scanner.setRange(range);
    iter = scanner.iterator();

  }

}
