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
package org.apache.accumulo.core.manager.state;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

/**
 * Object that represents a Tablets metadata and any actions that the Manager might need to take on
 * the object. This object is created by the TabletManagementIterator iterator used by the
 * TabletGroupWatcher threads in the Manager.
 *
 */
public class TabletManagement {

  public static final EnumSet<ColumnType> CONFIGURED_COLUMNS =
      EnumSet.of(ColumnType.PREV_ROW, ColumnType.LOCATION, ColumnType.SUSPEND, ColumnType.LOGS,
          ColumnType.HOSTING_GOAL, ColumnType.HOSTING_REQUESTED, ColumnType.FILES, ColumnType.LAST,
          ColumnType.OPID, ColumnType.ECOMP, ColumnType.DIR, ColumnType.SELECTED);

  private static final Text REASONS_COLUMN_NAME = new Text("REASONS");

  private static final Text EMPTY = new Text("");

  public static enum ManagementAction {
    BAD_STATE, NEEDS_COMPACTING, NEEDS_LOCATION_UPDATE, NEEDS_SPLITTING;
  }

  public static void addActions(final SortedMap<Key,Value> decodedRow,
      final Set<ManagementAction> actions) {
    final Key reasonsKey = new Key(decodedRow.firstKey().getRow(), REASONS_COLUMN_NAME, EMPTY);
    final Value reasonsValue = new Value(Joiner.on(',').join(actions));
    decodedRow.put(reasonsKey, reasonsValue);
  }

  public final Set<ManagementAction> actions;
  public final TabletMetadata tabletMetadata;

  public TabletManagement(Set<ManagementAction> actions, TabletMetadata tm) {
    this.actions = actions;
    this.tabletMetadata = tm;
  }

  public TabletManagement(Key wholeRowKey, Value wholeRowValue) throws IOException {
    this(wholeRowKey, wholeRowValue, false);
  }

  public TabletManagement(Key wholeRowKey, Value wholeRowValue, boolean saveKV) throws IOException {
    final SortedMap<Key,Value> decodedRow = WholeRowIterator.decodeRow(wholeRowKey, wholeRowValue);
    Text row = decodedRow.firstKey().getRow();
    Value val = decodedRow.remove(new Key(row, REASONS_COLUMN_NAME, EMPTY));
    Set<ManagementAction> actions = new HashSet<>();
    Splitter.on(',').split(val.toString()).forEach(a -> actions.add(ManagementAction.valueOf(a)));
    TabletMetadata tm = TabletMetadata.convertRow(decodedRow.entrySet().iterator(),
        CONFIGURED_COLUMNS, saveKV, true);
    this.actions = actions;
    this.tabletMetadata = tm;
  }

  public Set<ManagementAction> getActions() {
    return actions;
  }

  public TabletMetadata getTabletMetadata() {
    return tabletMetadata;
  }

  @Override
  public String toString() {
    return actions.toString() + "," + tabletMetadata.toString();
  }
}
