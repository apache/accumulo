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
package org.apache.accumulo.core.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.impl.AccumuloServerException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocations;
import org.apache.accumulo.core.client.impl.TabletLocatorImpl.TabletLocationObtainer;
import org.apache.accumulo.core.client.impl.TabletServerBatchReaderIterator;
import org.apache.accumulo.core.client.impl.TabletServerBatchReaderIterator.ResultReceiver;
import org.apache.accumulo.core.client.impl.ThriftScanner;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataLocationObtainer implements TabletLocationObtainer {
  private static final Logger log = LoggerFactory.getLogger(MetadataLocationObtainer.class);

  private SortedSet<Column> locCols;
  private ArrayList<Column> columns;

  public MetadataLocationObtainer() {

    locCols = new TreeSet<>();
    locCols.add(new Column(TextUtil.getBytes(TabletsSection.CurrentLocationColumnFamily.NAME), null, null));
    locCols.add(TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.toColumn());
    columns = new ArrayList<>(locCols);
  }

  @Override
  public TabletLocations lookupTablet(ClientContext context, TabletLocation src, Text row, Text stopRow, TabletLocator parent)
      throws AccumuloSecurityException, AccumuloException {

    try {

      OpTimer timer = null;

      if (log.isTraceEnabled()) {
        log.trace("tid={} Looking up in {} row={} extent={} tserver={}", Thread.currentThread().getId(), src.tablet_extent.getTableId(),
            TextUtil.truncate(row), src.tablet_extent, src.tablet_location);
        timer = new OpTimer().start();
      }

      Range range = new Range(row, true, stopRow, true);

      TreeMap<Key,Value> encodedResults = new TreeMap<>();
      TreeMap<Key,Value> results = new TreeMap<>();

      // Use the whole row iterator so that a partial mutations is not read. The code that extracts locations for tablets does a sanity check to ensure there is
      // only one location. Reading a partial mutation could make it appear there are multiple locations when there are not.
      List<IterInfo> serverSideIteratorList = new ArrayList<>();
      serverSideIteratorList.add(new IterInfo(10000, WholeRowIterator.class.getName(), "WRI"));
      Map<String,Map<String,String>> serverSideIteratorOptions = Collections.emptyMap();
      boolean more = ThriftScanner.getBatchFromServer(context, range, src.tablet_extent, src.tablet_location, encodedResults, locCols, serverSideIteratorList,
          serverSideIteratorOptions, Constants.SCAN_BATCH_SIZE, Authorizations.EMPTY, false, 0L, null);

      decodeRows(encodedResults, results);

      if (more && results.size() == 1) {
        range = new Range(results.lastKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), true, new Key(stopRow).followingKey(PartialKey.ROW), false);
        encodedResults.clear();
        more = ThriftScanner.getBatchFromServer(context, range, src.tablet_extent, src.tablet_location, encodedResults, locCols, serverSideIteratorList,
            serverSideIteratorOptions, Constants.SCAN_BATCH_SIZE, Authorizations.EMPTY, false, 0L, null);

        decodeRows(encodedResults, results);
      }

      if (timer != null) {
        timer.stop();
        log.trace("tid={} Got {} results from {} in {}", Thread.currentThread().getId(), results.size(), src.tablet_extent,
            String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
      }

      // if (log.isTraceEnabled()) log.trace("results "+results);

      return MetadataLocationObtainer.getMetadataLocationEntries(results);

    } catch (AccumuloServerException ase) {
      if (log.isTraceEnabled())
        log.trace("{} lookup failed, {} server side exception", src.tablet_extent.getTableId(), src.tablet_location);
      throw ase;
    } catch (NotServingTabletException e) {
      if (log.isTraceEnabled())
        log.trace("{} lookup failed, {} not serving {}", src.tablet_extent.getTableId(), src.tablet_location, src.tablet_extent);
      parent.invalidateCache(src.tablet_extent);
    } catch (AccumuloException e) {
      if (log.isTraceEnabled())
        log.trace("{} lookup failed", src.tablet_extent.getTableId(), e);
      parent.invalidateCache(context.getInstance(), src.tablet_location);
    }

    return null;
  }

  private void decodeRows(TreeMap<Key,Value> encodedResults, TreeMap<Key,Value> results) throws AccumuloException {
    for (Entry<Key,Value> entry : encodedResults.entrySet()) {
      try {
        results.putAll(WholeRowIterator.decodeRow(entry.getKey(), entry.getValue()));
      } catch (IOException e) {
        throw new AccumuloException(e);
      }
    }
  }

  private static class SettableScannerOptions extends ScannerOptions {
    public ScannerOptions setColumns(SortedSet<Column> locCols) {
      this.fetchedColumns = locCols;
      // see comment in lookupTablet about why iterator is used
      addScanIterator(new IteratorSetting(10000, "WRI", WholeRowIterator.class.getName()));
      return this;
    }
  }

  @Override
  public List<TabletLocation> lookupTablets(ClientContext context, String tserver, Map<KeyExtent,List<Range>> tabletsRanges, TabletLocator parent)
      throws AccumuloSecurityException, AccumuloException {

    final TreeMap<Key,Value> results = new TreeMap<>();

    ResultReceiver rr = new ResultReceiver() {

      @Override
      public void receive(List<Entry<Key,Value>> entries) {
        for (Entry<Key,Value> entry : entries) {
          try {
            results.putAll(WholeRowIterator.decodeRow(entry.getKey(), entry.getValue()));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    };

    ScannerOptions opts = null;
    try (SettableScannerOptions unsetOpts = new SettableScannerOptions()) {
      opts = unsetOpts.setColumns(locCols);
    }

    Map<KeyExtent,List<Range>> unscanned = new HashMap<>();
    Map<KeyExtent,List<Range>> failures = new HashMap<>();
    try {
      TabletServerBatchReaderIterator.doLookup(context, tserver, tabletsRanges, failures, unscanned, rr, columns, opts, Authorizations.EMPTY);
      if (failures.size() > 0) {
        // invalidate extents in parents cache
        if (log.isTraceEnabled())
          log.trace("lookupTablets failed for {} extents", failures.size());
        parent.invalidateCache(failures.keySet());
      }
    } catch (IOException e) {
      log.trace("lookupTablets failed server={}", tserver, e);
      parent.invalidateCache(context.getInstance(), tserver);
    } catch (AccumuloServerException e) {
      log.trace("lookupTablets failed server={}", tserver, e);
      throw e;
    }

    return MetadataLocationObtainer.getMetadataLocationEntries(results).getLocations();
  }

  public static TabletLocations getMetadataLocationEntries(SortedMap<Key,Value> entries) {
    Key key;
    Value val;
    Text location = null;
    Text session = null;
    Value prevRow = null;
    KeyExtent ke;

    List<TabletLocation> results = new ArrayList<>();
    ArrayList<KeyExtent> locationless = new ArrayList<>();

    Text lastRowFromKey = new Text();

    // text obj below is meant to be reused in loop for efficiency
    Text colf = new Text();
    Text colq = new Text();

    for (Entry<Key,Value> entry : entries.entrySet()) {
      key = entry.getKey();
      val = entry.getValue();

      if (key.compareRow(lastRowFromKey) != 0) {
        prevRow = null;
        location = null;
        session = null;
        key.getRow(lastRowFromKey);
      }

      colf = key.getColumnFamily(colf);
      colq = key.getColumnQualifier(colq);

      // interpret the row id as a key extent
      if (colf.equals(TabletsSection.CurrentLocationColumnFamily.NAME) || colf.equals(TabletsSection.FutureLocationColumnFamily.NAME)) {
        if (location != null) {
          throw new IllegalStateException("Tablet has multiple locations : " + lastRowFromKey);
        }
        location = new Text(val.toString());
        session = new Text(colq);
      } else if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.equals(colf, colq)) {
        prevRow = new Value(val);
      }

      if (prevRow != null) {
        ke = new KeyExtent(key.getRow(), prevRow);
        if (location != null)
          results.add(new TabletLocation(ke, location.toString(), session.toString()));
        else
          locationless.add(ke);

        location = null;
        prevRow = null;
      }
    }

    return new TabletLocations(results, locationless);
  }
}
