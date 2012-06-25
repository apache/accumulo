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
package org.apache.accumulo.core.client.impl;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.client.impl.TabletLocatorImpl.TabletLocationObtainer;
import org.apache.accumulo.core.client.impl.TabletServerBatchReaderIterator.ResultReceiver;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.util.MetadataTable;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class MetadataLocationObtainer implements TabletLocationObtainer {
  private static final Logger log = Logger.getLogger(MetadataLocationObtainer.class);
  private AuthInfo credentials;
  private SortedSet<Column> locCols;
  private ArrayList<Column> columns;
  private Instance instance;
  
  MetadataLocationObtainer(AuthInfo credentials, Instance instance) {
    
    this.instance = instance;
    this.credentials = credentials;
    
    locCols = new TreeSet<Column>();
    locCols.add(new Column(TextUtil.getBytes(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY), null, null));
    locCols.add(Constants.METADATA_PREV_ROW_COLUMN.toColumn());
    columns = new ArrayList<Column>(locCols);
  }
  
  @Override
  public List<TabletLocation> lookupTablet(TabletLocation src, Text row, Text stopRow, TabletLocator parent) throws AccumuloSecurityException,
      AccumuloException {
    
    ArrayList<TabletLocation> list = new ArrayList<TabletLocation>();
    
    try {
      OpTimer opTimer = null;
      if (log.isTraceEnabled())
        opTimer = new OpTimer(log, Level.TRACE).start("Looking up in " + src.tablet_extent.getTableId() + " row=" + TextUtil.truncate(row) + "  extent="
            + src.tablet_extent + " tserver=" + src.tablet_location);
      
      Range range = new Range(row, true, stopRow, true);
      
      TreeMap<Key,Value> results = new TreeMap<Key,Value>();
      
      // System.out.println(range);
      
      boolean more = ThriftScanner.getBatchFromServer(credentials, range, src.tablet_extent, src.tablet_location, results, locCols, Constants.SCAN_BATCH_SIZE,
          Constants.NO_AUTHS, false, instance.getConfiguration());
      if (more && results.size() == 1) {
        range = new Range(results.lastKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), true, new Key(stopRow).followingKey(PartialKey.ROW), false);
        more = ThriftScanner.getBatchFromServer(credentials, range, src.tablet_extent, src.tablet_location, results, locCols, Constants.SCAN_BATCH_SIZE,
            Constants.NO_AUTHS, false, instance.getConfiguration());
      }
      
      if (opTimer != null)
        opTimer.stop("Got " + results.size() + " results  from " + src.tablet_extent + " in %DURATION%");
      
      // System.out.println("results "+results.keySet());
      
      SortedMap<KeyExtent,Text> metadata = MetadataTable.getMetadataLocationEntries(results);
      
      for (Entry<KeyExtent,Text> entry : metadata.entrySet()) {
        list.add(new TabletLocation(entry.getKey(), entry.getValue().toString()));
      }
      
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
    
    return list;
  }
  
  @Override
  public List<TabletLocation> lookupTablets(String tserver, Map<KeyExtent,List<Range>> tabletsRanges, TabletLocator parent) throws AccumuloSecurityException,
      AccumuloException {
    
    final TreeMap<Key,Value> results = new TreeMap<Key,Value>();
    
    ArrayList<TabletLocation> list = new ArrayList<TabletLocation>();
    
    ResultReceiver rr = new ResultReceiver() {
      
      @Override
      public void receive(List<Entry<Key,Value>> entries) {
        for (Entry<Key,Value> entry : entries) {
          results.put(entry.getKey(), entry.getValue());
        }
      }
    };
    
    ScannerOptions opts = new ScannerOptions();
    opts.fetchedColumns = locCols;
    
    Map<KeyExtent,List<Range>> unscanned = new HashMap<KeyExtent,List<Range>>();
    Map<KeyExtent,List<Range>> failures = new HashMap<KeyExtent,List<Range>>();
    try {
      TabletServerBatchReaderIterator.doLookup(tserver, tabletsRanges, failures, unscanned, rr, columns, credentials, opts, Constants.NO_AUTHS,
          instance.getConfiguration());
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
    
    SortedMap<KeyExtent,Text> metadata = MetadataTable.getMetadataLocationEntries(results);
    
    for (Entry<KeyExtent,Text> entry : metadata.entrySet()) {
      list.add(new TabletLocation(entry.getKey(), entry.getValue().toString()));
    }
    
    return list;
  }
}
