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
import java.lang.ref.Cleaner.Cleanable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.cleaner.CleanerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabletManagementScanner implements ClosableIterator<TabletManagement> {

  private static final Logger log = LoggerFactory.getLogger(TabletManagementScanner.class);

  private final Cleanable cleanable;
  private final BatchScanner mdScanner;
  private final Iterator<Entry<Key,Value>> iter;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  TabletManagementScanner(ClientContext context, Range range, CurrentState state,
      String tableName) {
    // scan over metadata table, looking for tablets in the wrong state based on the live servers
    // and online tables
    try {
      mdScanner = context.createBatchScanner(tableName, Authorizations.EMPTY, 8);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException("Metadata table " + tableName + " should exist", e);
    }
    cleanable = CleanerUtil.unclosed(this, TabletManagementScanner.class, closed, log, mdScanner);
    TabletManagementIterator.configureScanner(mdScanner, state);
    mdScanner.setRanges(Collections.singletonList(range));
    iter = mdScanner.iterator();
  }

  public TabletManagementScanner(ClientContext context, Range range, String tableName) {
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
  public TabletManagement next() {
    if (closed.get()) {
      throw new NoSuchElementException(this.getClass().getSimpleName() + " is closed");
    }
    Entry<Key,Value> e = iter.next();
    try {
      TabletManagement tmi = TabletManagementIterator.decode(e);
      log.trace("Returning metadata tablet, extent: {}, hostingGoal: {}",
          tmi.getTabletMetadata().getExtent(), tmi.getTabletMetadata().getHostingGoal());
      return tmi;
    } catch (IOException e1) {
      throw new RuntimeException("Error creating TabletMetadata object", e1);
    }
  }

}
