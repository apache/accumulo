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
package org.apache.accumulo.server.manager.state;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.Cleaner.Cleanable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.cleaner.CleanerUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaDataTableScanner implements ClosableIterator<TabletLocationState> {
  private static final Logger log = LoggerFactory.getLogger(MetaDataTableScanner.class);

  private final Cleanable cleanable;
  private final BatchScanner mdScanner;
  private final Iterator<Entry<Key,Value>> iter;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  MetaDataTableScanner(ClientContext context, Range range, CurrentState state, String tableName) {
    // scan over metadata table, looking for tablets in the wrong state based on the live servers
    // and online tables
    try {
      mdScanner = context.createBatchScanner(tableName, Authorizations.EMPTY, 8);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException("Metadata table " + tableName + " should exist", e);
    }
    cleanable = CleanerUtil.unclosed(this, MetaDataTableScanner.class, closed, log, mdScanner);
    configureScanner(mdScanner, state);
    mdScanner.setRanges(Collections.singletonList(range));
    iter = mdScanner.iterator();
  }

  public static void configureScanner(ScannerBase scanner, CurrentState state) {
    TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
    scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
    scanner.fetchColumnFamily(FutureLocationColumnFamily.NAME);
    scanner.fetchColumnFamily(LastLocationColumnFamily.NAME);
    scanner.fetchColumnFamily(SuspendLocationColumn.SUSPEND_COLUMN.getColumnFamily());
    scanner.fetchColumnFamily(LogColumnFamily.NAME);
    scanner.addScanIterator(new IteratorSetting(1000, "wholeRows", WholeRowIterator.class));
    IteratorSetting tabletChange =
        new IteratorSetting(1001, "tabletChange", TabletStateChangeIterator.class);
    if (state != null) {
      TabletStateChangeIterator.setCurrentServers(tabletChange, state.onlineTabletServers());
      TabletStateChangeIterator.setOnlineTables(tabletChange, state.onlineTables());
      TabletStateChangeIterator.setMerges(tabletChange, state.merges());
      TabletStateChangeIterator.setMigrations(tabletChange, state.migrationsSnapshot());
      TabletStateChangeIterator.setManagerState(tabletChange, state.getManagerState());
      TabletStateChangeIterator.setShuttingDown(tabletChange, state.shutdownServers());
    }
    scanner.addScanIterator(tabletChange);
  }

  public MetaDataTableScanner(ClientContext context, Range range, String tableName) {
    this(context, range, null, tableName);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      // deregister cleanable, but it won't run because it checks
      // the value of closed first, which is now true
      cleanable.clean();
      mdScanner.close();
    }
  }

  @Override
  public boolean hasNext() {
    if (closed.get()) {
      return false;
    }
    boolean result = iter.hasNext();
    if (!result) {
      close();
    }
    return result;
  }

  @Override
  public TabletLocationState next() {
    if (closed.get()) {
      throw new NoSuchElementException(this.getClass().getSimpleName() + " is closed");
    }
    try {
      Entry<Key,Value> e = iter.next();
      return createTabletLocationState(e.getKey(), e.getValue());
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    } catch (BadLocationStateException ex) {
      throw new IllegalStateException(ex);
    }
  }

  public static TabletLocationState createTabletLocationState(Key k, Value v)
      throws IOException, BadLocationStateException {
    final SortedMap<Key,Value> decodedRow = WholeRowIterator.decodeRow(k, v);
    KeyExtent extent = null;
    Location future = null;
    Location current = null;
    Location last = null;
    SuspendingTServer suspend = null;
    long lastTimestamp = 0;
    List<Collection<String>> walogs = new ArrayList<>();

    for (Entry<Key,Value> entry : decodedRow.entrySet()) {

      Key key = entry.getKey();
      Text row = key.getRow();
      Text cf = key.getColumnFamily();
      Text cq = key.getColumnQualifier();

      if (cf.compareTo(FutureLocationColumnFamily.NAME) == 0) {
        Location location = Location.future(new TServerInstance(entry.getValue(), cq));
        if (future != null) {
          throw new BadLocationStateException("found two assignments for the same extent " + row
              + ": " + future + " and " + location, row);
        }
        future = location;
      } else if (cf.compareTo(CurrentLocationColumnFamily.NAME) == 0) {
        Location location = Location.current(new TServerInstance(entry.getValue(), cq));
        if (current != null) {
          throw new BadLocationStateException("found two locations for the same extent " + row
              + ": " + current + " and " + location, row);
        }
        current = location;
      } else if (cf.compareTo(LogColumnFamily.NAME) == 0) {
        walogs.add(Collections.singleton(LogEntry.fromMetaWalEntry(entry).getFilePath()));
      } else if (cf.compareTo(LastLocationColumnFamily.NAME) == 0) {
        if (lastTimestamp < entry.getKey().getTimestamp()) {
          last = Location.last(new TServerInstance(entry.getValue(), cq));
          lastTimestamp = entry.getKey().getTimestamp();
        }
      } else if (TabletColumnFamily.PREV_ROW_COLUMN.equals(cf, cq)) {
        extent = KeyExtent.fromMetaPrevRow(entry);
      } else if (SuspendLocationColumn.SUSPEND_COLUMN.equals(cf, cq)) {
        suspend = SuspendingTServer.fromValue(entry.getValue());
      }
    }
    if (extent == null) {
      String msg = "No prev-row for key extent " + decodedRow;
      log.error(msg);
      throw new BadLocationStateException(msg, k.getRow());
    }
    return new TabletLocationState(extent, future, current, last, suspend, walogs);
  }

}
