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

package org.apache.accumulo.core.metadata.schema;

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterators;

public class TabletMetadata {

  private Table.ID tableId;
  private Text prevEndRow;
  private Text endRow;
  private Location location;
  private List<String> files;
  private EnumSet<FetchedColumns> fetchedColumns;
  private KeyExtent extent;
  private Location last;

  public static enum LocationType {
    CURRENT, FUTURE, LAST
  }

  public static enum FetchedColumns {
    LOCATION, PREV_ROW, FILES, LAST
  }

  public static class Location {
    private final String server;
    private final String session;
    private final LocationType lt;

    Location(String server, String session, LocationType lt) {
      this.server = server;
      this.session = session;
      this.lt = lt;
    }

    public HostAndPort getHostAndPort() {
      return HostAndPort.fromString(server);
    }

    public String getSession() {
      return session;
    }

    public LocationType getLocationType() {
      return lt;
    }
  }

  public Table.ID getTableId() {
    return tableId;
  }

  public KeyExtent getExtent() {
    if (extent == null) {
      extent = new KeyExtent(getTableId(), getEndRow(), getPrevEndRow());
    }
    return extent;
  }

  public Text getPrevEndRow() {
    Preconditions.checkState(fetchedColumns.contains(FetchedColumns.PREV_ROW), "Requested prev row when it was not fetched");
    return prevEndRow;
  }

  public Text getEndRow() {
    return endRow;
  }

  public Location getLocation() {
    Preconditions.checkState(fetchedColumns.contains(FetchedColumns.LOCATION), "Requested location when it was not fetched");
    return location;
  }

  public Location getLast() {
    Preconditions.checkState(fetchedColumns.contains(FetchedColumns.LAST), "Requested last when it was not fetched");
    return last;
  }

  public List<String> getFiles() {
    Preconditions.checkState(fetchedColumns.contains(FetchedColumns.FILES), "Requested files when it was not fetched");
    return files;
  }

  public static TabletMetadata convertRow(Iterator<Entry<Key,Value>> rowIter, EnumSet<FetchedColumns> fetchedColumns) {
    Objects.requireNonNull(rowIter);

    TabletMetadata te = new TabletMetadata();

    Builder<String> filesBuilder = ImmutableList.builder();
    ByteSequence row = null;

    while (rowIter.hasNext()) {
      Entry<Key,Value> kv = rowIter.next();
      Key k = kv.getKey();
      Value v = kv.getValue();
      Text fam = k.getColumnFamily();

      if (row == null) {
        row = k.getRowData();
        KeyExtent ke = new KeyExtent(k.getRow(), (Text) null);
        te.endRow = ke.getEndRow();
        te.tableId = ke.getTableId();
      } else if (!row.equals(k.getRowData())) {
        throw new IllegalArgumentException("Input contains more than one row : " + row + " " + k.getRowData());
      }

      if (PREV_ROW_COLUMN.hasColumns(k)) {
        te.prevEndRow = KeyExtent.decodePrevEndRow(v);
      }

      if (fam.equals(DataFileColumnFamily.NAME)) {
        filesBuilder.add(k.getColumnQualifier().toString());
      } else if (fam.equals(CurrentLocationColumnFamily.NAME)) {
        if (te.location != null) {
          throw new IllegalArgumentException("Input contains more than one location " + te.location + " " + v);
        }
        te.location = new Location(v.toString(), k.getColumnQualifierData().toString(), LocationType.CURRENT);
      } else if (fam.equals(FutureLocationColumnFamily.NAME)) {
        if (te.location != null) {
          throw new IllegalArgumentException("Input contains more than one location " + te.location + " " + v);
        }
        te.location = new Location(v.toString(), k.getColumnQualifierData().toString(), LocationType.FUTURE);
      } else if (fam.equals(LastLocationColumnFamily.NAME)) {
        te.last = new Location(v.toString(), k.getColumnQualifierData().toString(), LocationType.LAST);
      }
    }

    te.files = filesBuilder.build();
    te.fetchedColumns = fetchedColumns;
    return te;
  }

  public static Iterable<TabletMetadata> convert(Scanner input, EnumSet<FetchedColumns> fetchedColumns) {
    return new Iterable<TabletMetadata>() {
      @Override
      public Iterator<TabletMetadata> iterator() {
        RowIterator rowIter = new RowIterator(input);
        return Iterators.transform(rowIter, ri -> convertRow(ri, fetchedColumns));
      }
    };
  }
}
