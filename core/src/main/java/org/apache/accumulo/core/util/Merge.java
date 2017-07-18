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
package org.apache.accumulo.core.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

public class Merge {

  public static class MergeException extends Exception {
    private static final long serialVersionUID = 1L;

    MergeException(Exception ex) {
      super(ex);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(Merge.class);

  protected void message(String format, Object... args) {
    log.info(String.format(format, args));
  }

  static class TextConverter implements IStringConverter<Text> {
    @Override
    public Text convert(String value) {
      return new Text(value);
    }
  }

  static class Opts extends ClientOnRequiredTable {
    @Parameter(names = {"-s", "--size"}, description = "merge goal size", converter = MemoryConverter.class)
    Long goalSize = null;
    @Parameter(names = {"-f", "--force"}, description = "merge small tablets even if merging them to larger tablets might cause a split")
    boolean force = false;
    @Parameter(names = {"-b", "--begin"}, description = "start tablet", converter = TextConverter.class)
    Text begin = null;
    @Parameter(names = {"-e", "--end"}, description = "end tablet", converter = TextConverter.class)
    Text end = null;
  }

  public void start(String[] args) throws MergeException {
    Opts opts = new Opts();
    opts.parseArgs(Merge.class.getName(), args);

    try {
      Connector conn = opts.getConnector();

      if (!conn.tableOperations().exists(opts.getTableName())) {
        System.err.println("table " + opts.getTableName() + " does not exist");
        return;
      }
      if (opts.goalSize == null || opts.goalSize < 1) {
        AccumuloConfiguration tableConfig = new ConfigurationCopy(conn.tableOperations().getProperties(opts.getTableName()));
        opts.goalSize = tableConfig.getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
      }

      message("Merging tablets in table %s to %d bytes", opts.getTableName(), opts.goalSize);
      mergomatic(conn, opts.getTableName(), opts.begin, opts.end, opts.goalSize, opts.force);
    } catch (Exception ex) {
      throw new MergeException(ex);
    }
  }

  public static void main(String[] args) throws MergeException {
    Merge merge = new Merge();
    merge.start(args);
  }

  public static class Size {
    public Size(KeyExtent extent, long size) {
      this.extent = extent;
      this.size = size;
    }

    KeyExtent extent;
    long size;
  }

  public void mergomatic(Connector conn, String table, Text start, Text end, long goalSize, boolean force) throws MergeException {
    try {
      if (table.equals(MetadataTable.NAME)) {
        throw new IllegalArgumentException("cannot merge tablets on the metadata table");
      }
      List<Size> sizes = new ArrayList<>();
      long totalSize = 0;
      // Merge any until you get larger than the goal size, and then merge one less tablet
      Iterator<Size> sizeIterator = getSizeIterator(conn, table, start, end);
      while (sizeIterator.hasNext()) {
        Size next = sizeIterator.next();
        totalSize += next.size;
        sizes.add(next);
        if (totalSize > goalSize) {
          totalSize = mergeMany(conn, table, sizes, goalSize, force, false);
        }
      }
      if (sizes.size() > 1)
        mergeMany(conn, table, sizes, goalSize, force, true);
    } catch (Exception ex) {
      throw new MergeException(ex);
    }
  }

  protected long mergeMany(Connector conn, String table, List<Size> sizes, long goalSize, boolean force, boolean last) throws MergeException {
    // skip the big tablets, which will be the typical case
    while (!sizes.isEmpty()) {
      if (sizes.get(0).size < goalSize)
        break;
      sizes.remove(0);
    }
    if (sizes.isEmpty()) {
      return 0;
    }

    // collect any small ones
    long mergeSize = 0;
    int numToMerge = 0;
    for (int i = 0; i < sizes.size(); i++) {
      if (mergeSize + sizes.get(i).size > goalSize) {
        numToMerge = i;
        break;
      }
      mergeSize += sizes.get(i).size;
    }

    if (numToMerge > 1) {
      mergeSome(conn, table, sizes, numToMerge);
    } else {
      if (numToMerge == 1 && sizes.size() > 1) {
        // here we have the case of a merge candidate that is surrounded by candidates that would split
        if (force) {
          mergeSome(conn, table, sizes, 2);
        } else {
          sizes.remove(0);
        }
      }
    }
    if (numToMerge == 0 && sizes.size() > 1 && last) {
      // That's the last tablet, and we have a bunch to merge
      mergeSome(conn, table, sizes, sizes.size());
    }
    long result = 0;
    for (Size s : sizes) {
      result += s.size;
    }
    return result;
  }

  protected void mergeSome(Connector conn, String table, List<Size> sizes, int numToMerge) throws MergeException {
    merge(conn, table, sizes, numToMerge);
    for (int i = 0; i < numToMerge; i++) {
      sizes.remove(0);
    }
  }

  protected void merge(Connector conn, String table, List<Size> sizes, int numToMerge) throws MergeException {
    try {
      Text start = sizes.get(0).extent.getPrevEndRow();
      Text end = sizes.get(numToMerge - 1).extent.getEndRow();
      message("Merging %d tablets from (%s to %s]", numToMerge, start == null ? "-inf" : start, end == null ? "+inf" : end);
      conn.tableOperations().merge(table, start, end);
    } catch (Exception ex) {
      throw new MergeException(ex);
    }
  }

  protected Iterator<Size> getSizeIterator(Connector conn, String tablename, Text start, Text end) throws MergeException {
    // open up metatadata, walk through the tablets.
    Table.ID tableId;
    Scanner scanner;
    try {
      tableId = Tables.getTableId(conn.getInstance(), tablename);
      scanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    } catch (Exception e) {
      throw new MergeException(e);
    }
    scanner.setRange(new KeyExtent(tableId, end, start).toMetadataRange());
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
    final Iterator<Entry<Key,Value>> iterator = scanner.iterator();

    Iterator<Size> result = new Iterator<Size>() {
      Size next = fetch();

      @Override
      public boolean hasNext() {
        return next != null;
      }

      private Size fetch() {
        long tabletSize = 0;
        while (iterator.hasNext()) {
          Entry<Key,Value> entry = iterator.next();
          Key key = entry.getKey();
          if (key.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
            tabletSize += new DataFileValue(entry.getValue().get()).getSize();
          } else if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)) {
            KeyExtent extent = new KeyExtent(key.getRow(), entry.getValue());
            return new Size(extent, tabletSize);
          }
        }
        return null;
      }

      @Override
      public Size next() {
        Size result = next;
        next = fetch();
        return result;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
    return result;
  }

}
