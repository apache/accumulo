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
package org.apache.accumulo.core.util;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

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

  public static class MemoryConverter implements IStringConverter<Long> {
    @Override
    public Long convert(String value) {
      return ConfigurationTypeHelper.getFixedMemoryAsBytes(value);
    }
  }

  static class TextConverter implements IStringConverter<Text> {
    @Override
    public Text convert(String value) {
      return new Text(value);
    }
  }

  static class Opts extends ClientOpts {
    @Parameter(names = {"-t", "--table"}, required = true, description = "table to use")
    String tableName;
    @Parameter(names = {"-s", "--size"}, description = "merge goal size",
        converter = MemoryConverter.class)
    Long goalSize = null;
    @Parameter(names = {"-f", "--force"},
        description = "merge small tablets even if merging them to larger"
            + " tablets might cause a split")
    boolean force = false;
    @Parameter(names = {"-b", "--begin"}, description = "start tablet",
        converter = TextConverter.class)
    Text begin = null;
    @Parameter(names = {"-e", "--end"}, description = "end tablet", converter = TextConverter.class)
    Text end = null;
  }

  public void start(String[] args) throws MergeException {
    Opts opts = new Opts();
    opts.parseArgs(Merge.class.getName(), args);
    Span span = TraceUtil.startSpan(Merge.class, "start");
    try (Scope scope = span.makeCurrent()) {

      try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {

        if (!client.tableOperations().exists(opts.tableName)) {
          System.err.println("table " + opts.tableName + " does not exist");
          return;
        }
        if (opts.goalSize == null || opts.goalSize < 1) {
          AccumuloConfiguration tableConfig =
              new ConfigurationCopy(client.tableOperations().getConfiguration(opts.tableName));
          opts.goalSize = tableConfig.getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
        }

        message("Merging tablets in table %s to %d bytes", opts.tableName, opts.goalSize);
        mergomatic(client, opts.tableName, opts.begin, opts.end, opts.goalSize, opts.force);
      } catch (Exception ex) {
        TraceUtil.setException(span, ex, true);
        throw new MergeException(ex);
      } finally {
        span.end();
      }
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

  public void mergomatic(AccumuloClient client, String table, Text start, Text end, long goalSize,
      boolean force) throws MergeException {
    try {
      if (table.equals(MetadataTable.NAME)) {
        throw new IllegalArgumentException("cannot merge tablets on the metadata table");
      }
      List<Size> sizes = new ArrayList<>();
      long totalSize = 0;
      // Merge any until you get larger than the goal size, and then merge one less tablet
      Iterator<Size> sizeIterator = getSizeIterator(client, table, start, end);
      while (sizeIterator.hasNext()) {
        Size next = sizeIterator.next();
        totalSize += next.size;
        sizes.add(next);
        if (totalSize > goalSize) {
          totalSize = mergeMany(client, table, sizes, goalSize, force, false);
        }
      }
      if (sizes.size() > 1) {
        mergeMany(client, table, sizes, goalSize, force, true);
      }
    } catch (Exception ex) {
      throw new MergeException(ex);
    }
  }

  protected long mergeMany(AccumuloClient client, String table, List<Size> sizes, long goalSize,
      boolean force, boolean last) throws MergeException {
    // skip the big tablets, which will be the typical case
    while (!sizes.isEmpty()) {
      if (sizes.get(0).size < goalSize) {
        break;
      }
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
      mergeSome(client, table, sizes, numToMerge);
    } else {
      if (numToMerge == 1 && sizes.size() > 1) {
        // here we have the case of a merge candidate that is surrounded by candidates that would
        // split
        if (force) {
          mergeSome(client, table, sizes, 2);
        } else {
          sizes.remove(0);
        }
      }
    }
    if (numToMerge == 0 && sizes.size() > 1 && last) {
      // That's the last tablet, and we have a bunch to merge
      mergeSome(client, table, sizes, sizes.size());
    }
    long result = 0;
    for (Size s : sizes) {
      result += s.size;
    }
    return result;
  }

  protected void mergeSome(AccumuloClient client, String table, List<Size> sizes, int numToMerge)
      throws MergeException {
    merge(client, table, sizes, numToMerge);
    for (int i = 0; i < numToMerge; i++) {
      sizes.remove(0);
    }
  }

  protected void merge(AccumuloClient client, String table, List<Size> sizes, int numToMerge)
      throws MergeException {
    try {
      Text start = sizes.get(0).extent.prevEndRow();
      Text end = sizes.get(numToMerge - 1).extent.endRow();
      message("Merging %d tablets from (%s to %s]", numToMerge,
          start == null ? "-inf"
              : Key.toPrintableString(start.getBytes(), 0, start.getLength(), start.getLength()),
          end == null ? "+inf"
              : Key.toPrintableString(end.getBytes(), 0, end.getLength(), end.getLength()));
      client.tableOperations().merge(table, start, end);
    } catch (Exception ex) {
      throw new MergeException(ex);
    }
  }

  protected Iterator<Size> getSizeIterator(AccumuloClient client, String tablename, Text start,
      Text end) throws MergeException {
    // open up metadata, walk through the tablets.

    TableId tableId;
    TabletsMetadata tablets;
    try {
      ClientContext context = (ClientContext) client;
      tableId = context.getTableId(tablename);
      tablets = TabletsMetadata.builder(context).scanMetadataTable()
          .overRange(new KeyExtent(tableId, end, start).toMetaRange()).fetch(FILES, PREV_ROW)
          .build();
    } catch (Exception e) {
      throw new MergeException(e);
    }

    return tablets.stream().map(tm -> {
      long size = tm.getFilesMap().values().stream().mapToLong(DataFileValue::getSize).sum();
      return new Size(tm.getExtent(), size);
    }).iterator();
  }

}
