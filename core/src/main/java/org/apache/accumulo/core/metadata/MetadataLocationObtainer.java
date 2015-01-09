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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.AccumuloServerException;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.client.impl.ServerConfigurationUtil;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocations;
import org.apache.accumulo.core.client.impl.TabletLocatorImpl.TabletLocationObtainer;
import org.apache.accumulo.core.client.impl.TabletServerBatchReaderIterator;
import org.apache.accumulo.core.client.impl.TabletServerBatchReaderIterator.ResultReceiver;
import org.apache.accumulo.core.client.impl.ThriftScanner;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class MetadataLocationObtainer implements TabletLocationObtainer {
  private static final Logger log = Logger.getLogger(MetadataLocationObtainer.class);
  private SortedSet<Column> locCols;
  private ArrayList<Column> columns;
  private Instance instance;

  public MetadataLocationObtainer(Instance instance) {

    this.instance = instance;

    locCols = new TreeSet<Column>();
    locCols.add(new Column(TextUtil.getBytes(TabletsSection.CurrentLocationColumnFamily.NAME), null, null));
    locCols.add(TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.toColumn());
    columns = new ArrayList<Column>(locCols);
  }

  @Override
  public TabletLocations lookupTablet(Credentials credentials, TabletLocation src, Text row, Text stopRow, TabletLocator parent)
      throws AccumuloSecurityException, AccumuloException {

    try {
      OpTimer opTimer = null;
      if (log.isTraceEnabled())
        opTimer = new OpTimer(log, Level.TRACE).start("Looking up in " + src.tablet_extent.getTableId() + " row=" + TextUtil.truncate(row) + "  extent="
            + src.tablet_extent + " tserver=" + src.tablet_location);

      Range range = new Range(row, true, stopRow, true);

      TreeMap<Key,Value> encodedResults = new TreeMap<Key,Value>();
      TreeMap<Key,Value> results = new TreeMap<Key,Value>();

      // Use the whole row iterator so that a partial mutations is not read. The code that extracts locations for tablets does a sanity check to ensure there is
      // only one location. Reading a partial mutation could make it appear there are multiple locations when there are not.
      List<IterInfo> serverSideIteratorList = new ArrayList<IterInfo>();
      serverSideIteratorList.add(new IterInfo(10000, WholeRowIterator.class.getName(), "WRI"));
      Map<String,Map<String,String>> serverSideIteratorOptions = Collections.emptyMap();

      boolean more = ThriftScanner.getBatchFromServer(instance, credentials, range, src.tablet_extent, src.tablet_location, encodedResults, locCols,
          serverSideIteratorList, serverSideIteratorOptions, Constants.SCAN_BATCH_SIZE, Authorizations.EMPTY, false,
          ServerConfigurationUtil.getConfiguration(instance));

      decodeRows(encodedResults, results);

      if (more && results.size() == 1) {
        range = new Range(results.lastKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), true, new Key(stopRow).followingKey(PartialKey.ROW), false);
        encodedResults.clear();
        more = ThriftScanner.getBatchFromServer(instance, credentials, range, src.tablet_extent, src.tablet_location, encodedResults, locCols,
            serverSideIteratorList, serverSideIteratorOptions, Constants.SCAN_BATCH_SIZE, Authorizations.EMPTY, false,
            ServerConfigurationUtil.getConfiguration(instance));

        decodeRows(encodedResults, results);
      }

      if (opTimer != null)
        opTimer.stop("Got " + results.size() + " results  from " + src.tablet_extent + " in %DURATION%");

      // if (log.isTraceEnabled()) log.trace("results "+results);

      return MetadataLocationObtainer.getMetadataLocationEntries(results);

    } catch (AccumuloServerException ase) {
      if (log.isTraceEnabled())
        log.trace(src.tablet_extent.getTableId() + " lookup failed, " + src.tablet_location + " server side exception");
      throw ase;
    } catch (NotServingTabletException e) {
      if (log.isTraceEnabled())
        log.trace(src.tablet_extent.getTableId() + " lookup failed, " + src.tablet_location + " not serving " + src.tablet_extent);
      parent.invalidateCache(src.tablet_extent);
    } catch (AccumuloException e) {
      if (log.isTraceEnabled())
        log.trace(src.tablet_extent.getTableId() + " lookup failed", e);
      parent.invalidateCache(src.tablet_location);
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

  @Override
  public List<TabletLocation> lookupTablets(Credentials credentials, String tserver, Map<KeyExtent,List<Range>> tabletsRanges, TabletLocator parent)
      throws AccumuloSecurityException, AccumuloException {

    final TreeMap<Key,Value> results = new TreeMap<Key,Value>();

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

    ScannerOptions opts = new ScannerOptions() {
      ScannerOptions setOpts() {
        this.fetchedColumns = locCols;
        this.serverSideIteratorList = new ArrayList<IterInfo>();
        // see comment in lookupTablet about why iterator is used
        this.serverSideIteratorList.add(new IterInfo(10000, WholeRowIterator.class.getName(), "WRI"));
        return this;
      }
    }.setOpts();

    Map<KeyExtent,List<Range>> unscanned = new HashMap<KeyExtent,List<Range>>();
    Map<KeyExtent,List<Range>> failures = new HashMap<KeyExtent,List<Range>>();
    try {
      TabletServerBatchReaderIterator.doLookup(instance, credentials, tserver, tabletsRanges, failures, unscanned, rr, columns, opts, Authorizations.EMPTY,
          ServerConfigurationUtil.getConfiguration(instance));
      if (failures.size() > 0) {
        // invalidate extents in parents cache
        if (log.isTraceEnabled())
          log.trace("lookupTablets failed for " + failures.size() + " extents");
        parent.invalidateCache(failures.keySet());
      }
    } catch (IOException e) {
      log.trace("lookupTablets failed server=" + tserver, e);
      parent.invalidateCache(tserver);
    } catch (AccumuloServerException e) {
      log.trace("lookupTablets failed server=" + tserver, e);
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

    List<TabletLocation> results = new ArrayList<TabletLocation>();
    ArrayList<KeyExtent> locationless = new ArrayList<KeyExtent>();

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
