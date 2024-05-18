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
package org.apache.accumulo.server.split;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.UnSplittableMetadata;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

public class SplitUtils {

  private static final Logger log = LoggerFactory.getLogger(SplitUtils.class);

  static class IndexIterator implements Iterator<Key> {

    private final SortedKeyValueIterator<Key,Value> source;

    private final Text prevEndRow;
    private final Text endRow;

    public IndexIterator(SortedKeyValueIterator<Key,Value> source, Text endRow, Text prevEndRow) {
      this.source = source;
      this.prevEndRow = prevEndRow;
      this.endRow = endRow;
    }

    @Override
    public boolean hasNext() {
      if (prevEndRow != null) {
        // this code filters out data because the rfile index iterators do not support seek, so just
        // discard everything before our point of interest.
        while (source.hasTop() && source.getTopKey().getRow().compareTo(prevEndRow) <= 0) {
          try {
            source.next();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }

      if (endRow != null) {
        return source.hasTop() && source.getTopKey().getRow().compareTo(endRow) <= 0;
      }

      return source.hasTop();
    }

    @Override
    public Key next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Key key = source.getTopKey();

      try {
        source.next();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      return key;
    }
  }

  private static ArrayList<FileSKVIterator> openIndexes(ServerContext context,
      TableConfiguration tableConf, Collection<StoredTabletFile> files) throws IOException {

    ArrayList<FileSKVIterator> readers = new ArrayList<>();

    try {
      for (TabletFile file : files) {
        FileSKVIterator reader = null;
        FileSystem ns = context.getVolumeManager().getFileSystemByPath(file.getPath());

        reader = FileOperations.getInstance().newIndexReaderBuilder()
            .forFile(file, ns, ns.getConf(), tableConf.getCryptoService())
            .withTableConfiguration(tableConf).build();

        readers.add(reader);
      }
    } catch (IOException ioe) {
      readers.forEach(reader -> {
        try {
          reader.close();
        } catch (IOException e) {
          log.debug("failed to close reader", e);
        }
      });
      throw ioe;
    }

    return readers;
  }

  public static class IndexIterable implements AutoCloseable, Iterable<Key> {

    private final ServerContext context;
    private final TableConfiguration tableConf;
    private final Collection<StoredTabletFile> files;
    private final Text prevEndRow;
    private final Text endRow;
    private ArrayList<FileSKVIterator> readers;

    public IndexIterable(ServerContext context, TableConfiguration tableConf,
        Collection<StoredTabletFile> files, Text endRow, Text prevEndRow) {
      this.context = context;
      this.tableConf = tableConf;
      this.files = files;
      this.prevEndRow = prevEndRow;
      this.endRow = endRow;
    }

    @Override
    public Iterator<Key> iterator() {
      close();
      try {
        readers = openIndexes(context, tableConf, files);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(readers);
      MultiIterator mmfi = new MultiIterator(iters, true);

      return new IndexIterator(mmfi, endRow, prevEndRow);
    }

    @Override
    public void close() {
      if (readers != null) {
        readers.forEach(reader -> {
          try {
            reader.close();
          } catch (IOException e) {
            log.debug("Failed to close index reader", e);
          }
        });
        readers = null;
      }
    }
  }

  public static int calculateDesiredSplits(long esitimatedSize, long splitThreshold) {
    return (int) Math.floor((double) esitimatedSize / (double) splitThreshold);
  }

  public static SortedSet<Text> findSplits(ServerContext context, TabletMetadata tabletMetadata) {
    var tableConf = context.getTableConfiguration(tabletMetadata.getTableId());
    var threshold = tableConf.getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
    var maxEndRowSize = tableConf.getAsBytes(Property.TABLE_MAX_END_ROW_SIZE);

    int maxFilesToOpen = tableConf.getCount(Property.SPLIT_MAXOPEN);

    var estimatedSize = tabletMetadata.getFileSize();
    if (!needsSplit(context, tabletMetadata)) {
      return new TreeSet<>();
    }

    if (tabletMetadata.getFiles().size() >= maxFilesToOpen) {
      log.warn("Tablet {} has {} files which exceeds the max to open for split, so can not split.",
          tabletMetadata.getExtent(), tabletMetadata.getFiles().size());
      return new TreeSet<>();
    }

    try (var indexIterable = new IndexIterable(context, tableConf, tabletMetadata.getFiles(),
        tabletMetadata.getEndRow(), tabletMetadata.getPrevEndRow())) {

      Predicate<ByteSequence> splitPredicate = splitCandidate -> {
        if (splitCandidate.length() >= maxEndRowSize) {
          log.warn("Ignoring split point for {} of length {}", tabletMetadata.getExtent(),
              splitCandidate.length());
          return false;
        }

        return true;
      };

      return findSplits(indexIterable, calculateDesiredSplits(estimatedSize, threshold),
          splitPredicate);
    }
  }

  private static int longestCommonLength(ByteSequence bs1, ByteSequence bs2) {
    int common = 0;
    while (common < bs1.length() && common < bs2.length()
        && bs1.byteAt(common) == bs2.byteAt(common)) {
      common++;
    }
    return common;
  }

  public static SortedSet<Text> findSplits(Iterable<Key> tabletIndexIterator, int desiredSplits,
      Predicate<ByteSequence> rowPredicate) {
    Preconditions.checkArgument(desiredSplits >= 1);

    int numKeys = Iterables.size(tabletIndexIterator);

    double interSplitDistance = (double) numKeys / (double) (desiredSplits + 1);

    SortedSet<Text> splits = new TreeSet<>();

    long count = 0;

    ByteSequence prevRow = null;
    ByteSequence lastRow = null;

    for (Key key : tabletIndexIterator) {
      if (lastRow != null && !key.getRowData().equals(lastRow)) {
        prevRow = lastRow;
      }

      count++;

      if (count >= Math.round((splits.size() + 1) * interSplitDistance)) {
        if (prevRow == null) {
          if (rowPredicate.test(key.getRowData())) {
            splits.add(key.getRow());
          }
        } else {
          var lcl = longestCommonLength(prevRow, key.getRowData());
          if (lcl + 1 >= key.getRowData().length()) {
            if (rowPredicate.test(key.getRowData())) {
              splits.add(key.getRow());
            }
          } else {
            var shortenedRow = key.getRowData().subSequence(0, lcl + 1);
            if (rowPredicate.test(shortenedRow)) {
              splits.add(new Text(shortenedRow.toArray()));
            }
          }
        }

        if (splits.size() >= desiredSplits) {
          break;
        }
      }

      lastRow = key.getRowData();
    }

    return splits;
  }

  public static boolean needsSplit(ServerContext context, TabletMetadata tabletMetadata) {
    var tableConf = context.getTableConfiguration(tabletMetadata.getTableId());
    var splitThreshold = tableConf.getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
    return needsSplit(splitThreshold, tabletMetadata);
  }

  public static boolean needsSplit(long splitThreshold, TabletMetadata tabletMetadata) {
    return tabletMetadata.getFileSize() > splitThreshold;
  }

  public static UnSplittableMetadata toUnSplittable(ServerContext context,
      TabletMetadata tabletMetadata) {
    var tableConf = context.getTableConfiguration(tabletMetadata.getTableId());
    var splitThreshold = tableConf.getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
    var maxEndRowSize = tableConf.getAsBytes(Property.TABLE_MAX_END_ROW_SIZE);
    int maxFilesToOpen = tableConf.getCount(Property.SPLIT_MAXOPEN);

    var unSplittableMetadata = UnSplittableMetadata.toUnSplittable(tabletMetadata.getExtent(),
        splitThreshold, maxEndRowSize, maxFilesToOpen, tabletMetadata.getFiles());

    log.trace(
        "Created unsplittable metadata for tablet {}. splitThreshold: {}, maxEndRowSize:{}, maxFilesToOpen: {}, hashCode: {}",
        tabletMetadata.getExtent(), splitThreshold, maxEndRowSize, maxFilesToOpen,
        unSplittableMetadata);

    return unSplittableMetadata;
  }

}
