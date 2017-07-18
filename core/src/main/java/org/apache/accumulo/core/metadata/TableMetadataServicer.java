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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * A {@link MetadataServicer} that is backed by a table
 */
abstract class TableMetadataServicer extends MetadataServicer {

  private final ClientContext context;
  private Table.ID tableIdBeingServiced;
  private String serviceTableName;

  public TableMetadataServicer(ClientContext context, String serviceTableName, Table.ID tableIdBeingServiced) {
    this.context = context;
    this.serviceTableName = serviceTableName;
    this.tableIdBeingServiced = tableIdBeingServiced;
  }

  @Override
  public Table.ID getServicedTableId() {
    return tableIdBeingServiced;
  }

  public String getServicingTableName() {
    return serviceTableName;
  }

  @Override
  public void getTabletLocations(SortedMap<KeyExtent,String> tablets) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    Scanner scanner = context.getConnector().createScanner(getServicingTableName(), Authorizations.EMPTY);

    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
    scanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);

    // position at first entry in metadata table for given table
    scanner.setRange(TabletsSection.getRange(getServicedTableId()));

    Text colf = new Text();
    Text colq = new Text();

    KeyExtent currentKeyExtent = null;
    String location = null;
    Text row = null;
    // acquire this table's tablets from the metadata table which services it
    for (Entry<Key,Value> entry : scanner) {
      if (row != null) {
        if (!row.equals(entry.getKey().getRow())) {
          currentKeyExtent = null;
          location = null;
          row = entry.getKey().getRow();
        }
      } else {
        row = entry.getKey().getRow();
      }

      colf = entry.getKey().getColumnFamily(colf);
      colq = entry.getKey().getColumnQualifier(colq);

      if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.equals(colf, colq)) {
        currentKeyExtent = new KeyExtent(entry.getKey().getRow(), entry.getValue());
        tablets.put(currentKeyExtent, location);
        currentKeyExtent = null;
        location = null;
      } else if (colf.equals(TabletsSection.CurrentLocationColumnFamily.NAME)) {
        location = entry.getValue().toString();
      }

    }

    validateEntries(tablets);
  }

  private void validateEntries(SortedMap<KeyExtent,String> tablets) throws AccumuloException {
    SortedSet<KeyExtent> tabletsKeys = (SortedSet<KeyExtent>) tablets.keySet();
    // sanity check of metadata table entries
    // make sure tablets has no holes, and that it starts and ends w/ null
    if (tabletsKeys.size() == 0)
      throw new AccumuloException("No entries found in metadata table for table " + getServicedTableId());

    if (tabletsKeys.first().getPrevEndRow() != null)
      throw new AccumuloException("Problem with metadata table, first entry for table " + getServicedTableId() + "- " + tabletsKeys.first()
          + " - has non null prev end row");

    if (tabletsKeys.last().getEndRow() != null)
      throw new AccumuloException("Problem with metadata table, last entry for table " + getServicedTableId() + "- " + tabletsKeys.first()
          + " - has non null end row");

    Iterator<KeyExtent> tabIter = tabletsKeys.iterator();
    Text lastEndRow = tabIter.next().getEndRow();
    while (tabIter.hasNext()) {
      KeyExtent tabke = tabIter.next();

      if (tabke.getPrevEndRow() == null)
        throw new AccumuloException("Problem with metadata table, it has null prev end row in middle of table " + tabke);

      if (!tabke.getPrevEndRow().equals(lastEndRow))
        throw new AccumuloException("Problem with metadata table, it has a hole " + tabke.getPrevEndRow() + " != " + lastEndRow);

      lastEndRow = tabke.getEndRow();
    }

    // end METADATA table sanity check
  }

}
