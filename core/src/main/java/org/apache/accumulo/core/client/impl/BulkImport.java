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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations.ImportExecutorOptions;
import org.apache.accumulo.core.client.admin.TableOperations.ImportSourceArguments;
import org.apache.accumulo.core.client.admin.TableOperations.ImportSourceOptions;
import org.apache.accumulo.core.client.impl.Table.ID;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.master.thrift.FateOperation;
import org.apache.accumulo.core.metadata.schema.MetadataScanner;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class BulkImport implements ImportSourceArguments, ImportExecutorOptions {

  private static final Logger log = LoggerFactory.getLogger(BulkImport.class);

  private boolean setTime = false;
  private Executor executor = null;
  private String dir;
  private int numThreads = -1;

  private final ClientContext context;
  private final String tableName;

  BulkImport(String tableName, ClientContext context) {
    this.context = context;
    this.tableName = Objects.requireNonNull(tableName);
  }

  @Override
  public ImportSourceOptions settingLogicalTime() {
    this.setTime = true;
    return this;
  }

  @Override
  public void load()
      throws TableNotFoundException, IOException, AccumuloException, AccumuloSecurityException {

    Table.ID tableId = Tables.getTableId(context.getInstance(), tableName);

    Map<String,String> props = context.getConnector().instanceOperations().getSystemConfiguration();
    AccumuloConfiguration conf = new ConfigurationCopy(props);

    FileSystem fs = VolumeConfiguration.getVolume(dir, CachedConfiguration.getInstance(), conf)
        .getFileSystem();

    Path srcPath = checkPath(fs, dir);

    Executor executor;
    ExecutorService service = null;

    if (this.executor != null) {
      executor = this.executor;
    } else if (numThreads > 0) {
      executor = service = Executors.newFixedThreadPool(numThreads);
    } else {
      String threads = context.getConfiguration().get(ClientProperty.BULK_LOAD_THREADS.getKey());
      executor = service = Executors
          .newFixedThreadPool(ConfigurationTypeHelper.getNumThreads(threads));
    }

    try {
      SortedMap<KeyExtent,Bulk.Files> mappings = computeFileToTabletMappings(fs, tableId, srcPath,
          executor, context);

      BulkSerialize.writeLoadMapping(mappings, srcPath.toString(), p -> fs.create(p));

      List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableId.getUtf8()),
          ByteBuffer.wrap(srcPath.toString().getBytes(UTF_8)),
          ByteBuffer.wrap((setTime + "").getBytes(UTF_8)));
      doFateOperation(FateOperation.TABLE_BULK_IMPORT2, args, Collections.emptyMap(), tableName);
    } finally {
      if (service != null) {
        service.shutdown();
      }
    }
  }

  /**
   * Check path of bulk directory and permissions
   */
  private Path checkPath(FileSystem fs, String dir) throws IOException, AccumuloException {
    Path ret;

    if (dir.contains(":")) {
      ret = new Path(dir);
    } else {
      ret = fs.makeQualified(new Path(dir));
    }

    try {
      if (!fs.getFileStatus(ret).isDirectory()) {
        throw new AccumuloException("Bulk import directory " + dir + " is not a directory!");
      }
      Path tmpFile = new Path(ret, "isWritable");
      if (fs.createNewFile(tmpFile))
        fs.delete(tmpFile, true);
      else
        throw new AccumuloException("Bulk import directory " + dir + " is not writable.");
    } catch (FileNotFoundException fnf) {
      throw new AccumuloException(
          "Bulk import directory " + dir + " does not exist or has bad permissions", fnf);
    }
    return ret;
  }

  @Override
  public ImportSourceOptions usingExecutor(Executor service) {
    this.executor = Objects.requireNonNull(service);
    return this;
  }

  @Override
  public ImportSourceOptions usingThreads(int numThreads) {
    Preconditions.checkArgument(numThreads > 0, "Non positive number of threads given : %s",
        numThreads);
    this.numThreads = numThreads;
    return this;
  }

  @Override
  public ImportExecutorOptions from(String directory) {
    this.dir = Objects.requireNonNull(directory);
    return this;
  }

  private final static byte[] byte0 = {0};

  private static class MLong {
    public MLong(long i) {
      l = i;
    }

    long l;
  }

  public static Map<KeyExtent,Long> estimateSizes(AccumuloConfiguration acuConf, Path mapFile,
      long fileSize, Collection<KeyExtent> extents, FileSystem ns) throws IOException {

    long totalIndexEntries = 0;
    Map<KeyExtent,MLong> counts = new TreeMap<>();
    for (KeyExtent keyExtent : extents)
      counts.put(keyExtent, new MLong(0));

    Text row = new Text();

    FileSKVIterator index = FileOperations.getInstance().newIndexReaderBuilder()
        .forFile(mapFile.toString(), ns, ns.getConf()).withTableConfiguration(acuConf).build();

    try {
      while (index.hasTop()) {
        Key key = index.getTopKey();
        totalIndexEntries++;
        key.getRow(row);

        // TODO this could use a binary search
        for (Entry<KeyExtent,MLong> entry : counts.entrySet())
          if (entry.getKey().contains(row))
            entry.getValue().l++;

        index.next();
      }
    } finally {
      try {
        if (index != null)
          index.close();
      } catch (IOException e) {
        log.debug("Failed to close " + mapFile, e);
      }
    }

    Map<KeyExtent,Long> results = new TreeMap<>();
    for (KeyExtent keyExtent : extents) {
      double numEntries = counts.get(keyExtent).l;
      if (numEntries == 0)
        numEntries = 1;
      long estSize = (long) ((numEntries / totalIndexEntries) * fileSize);
      results.put(keyExtent, estSize);
    }
    return results;
  }

  public interface KeyExtentCache {
    KeyExtent lookup(Text row)
        throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException;
  }

  @VisibleForTesting
  static class ConcurrentKeyExtentCache implements KeyExtentCache {

    private static final Text MAX = new Text();

    private Set<Text> rowsToLookup = Collections.synchronizedSet(new HashSet<>());

    List<Text> lookupRows = new ArrayList<>();

    private ConcurrentSkipListMap<Text,KeyExtent> extents = new ConcurrentSkipListMap<>(
        (t1, t2) -> {
          return (t1 == t2) ? 0 : (t1 == MAX ? 1 : (t2 == MAX ? -1 : t1.compareTo(t2)));
        });
    private ID tableId;
    private ClientContext ctx;

    @VisibleForTesting
    ConcurrentKeyExtentCache(Table.ID tableId, ClientContext ctx) {
      this.tableId = tableId;
      this.ctx = ctx;
    }

    private KeyExtent getFromCache(Text row) {
      Entry<Text,KeyExtent> entry = extents.ceilingEntry(row);
      if (entry != null && entry.getValue().contains(row)) {
        return entry.getValue();
      }

      return null;
    }

    private boolean inCache(KeyExtent e) {
      return Objects.equals(e, extents.get(e.getEndRow() == null ? MAX : e.getEndRow()));
    }

    @VisibleForTesting
    protected void updateCache(KeyExtent e) {
      Text prevRow = e.getPrevEndRow() == null ? new Text() : e.getPrevEndRow();
      Text endRow = e.getEndRow() == null ? MAX : e.getEndRow();
      extents.subMap(prevRow, e.getPrevEndRow() == null, endRow, true).clear();
      extents.put(endRow, e);
    }

    @VisibleForTesting
    protected Stream<KeyExtent> lookupExtents(Text row)
        throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
      return MetadataScanner.builder().from(ctx).scanMetadataTable().overRange(tableId, row, null)
          .checkConsistency().fetchPrev().build().stream().limit(100)
          .map(TabletMetadata::getExtent);
    }

    @Override
    public KeyExtent lookup(Text row)
        throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
      while (true) {
        KeyExtent ke = getFromCache(row);
        if (ke != null)
          return ke;

        // If a metadata lookup is currently in progress, then multiple threads can queue up their
        // rows. The next lookup will process all queued. Processing multiple at once can be more
        // efficient.
        rowsToLookup.add(row);

        synchronized (this) {
          // This check is done to avoid processing rowsToLookup when the current thread's row is in
          // the cache.
          ke = getFromCache(row);
          if (ke != null) {
            rowsToLookup.remove(row);
            return ke;
          }

          lookupRows.clear();
          synchronized (rowsToLookup) {
            // Gather all rows that were queued for lookup before this point in time.
            rowsToLookup.forEach(lookupRows::add);
            rowsToLookup.clear();
          }
          // Lookup rows in the metadata table in sorted order. This could possibly lead to less
          // metadata lookups.
          lookupRows.sort(Text::compareTo);

          for (Text lookupRow : lookupRows) {
            if (getFromCache(lookupRow) == null) {
              Iterator<KeyExtent> iter = lookupExtents(lookupRow).iterator();
              while (iter.hasNext()) {
                KeyExtent ke2 = iter.next();
                if (inCache(ke2))
                  break;
                updateCache(ke2);
              }
            }
          }
        }
      }
    }
  }

  public static List<KeyExtent> findOverlappingTablets(ClientContext context,
      KeyExtentCache extentCache, Text startRow, Text endRow, FileSKVIterator reader)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    List<KeyExtent> result = new ArrayList<>();
    Collection<ByteSequence> columnFamilies = Collections.emptyList();
    Text row = startRow;
    if (row == null)
      row = new Text();
    while (true) {
      // log.debug(filename + " Seeking to row " + row);
      reader.seek(new Range(row, null), columnFamilies, false);
      if (!reader.hasTop()) {
        // log.debug(filename + " not found");
        break;
      }
      row = reader.getTopKey().getRow();
      KeyExtent extent = extentCache.lookup(row);
      // log.debug(filename + " found row " + row + " at location " + tabletLocation);
      result.add(extent);
      row = extent.getEndRow();
      if (row != null && (endRow == null || row.compareTo(endRow) < 0)) {
        row = new Text(row);
        row.append(byte0, 0, byte0.length);
      } else
        break;
    }

    return result;
  }

  public static List<KeyExtent> findOverlappingTablets(ClientContext context,
      KeyExtentCache extentCache, Path file, FileSystem fs)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    try (FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder()
        .forFile(file.toString(), fs, fs.getConf())
        .withTableConfiguration(context.getConfiguration()).seekToBeginning().build()) {
      return findOverlappingTablets(context, extentCache, null, null, reader);
    }
  }

  public static SortedMap<KeyExtent,Bulk.Files> computeFileToTabletMappings(FileSystem fs,
      Table.ID tableId, Path dirPath, Executor executor, ClientContext context) throws IOException {

    KeyExtentCache extentCache = new ConcurrentKeyExtentCache(tableId, context);

    FileStatus[] files = fs.listStatus(dirPath,
        p -> !p.getName().equals(Constants.BULK_LOAD_MAPPING));

    List<CompletableFuture<Map<KeyExtent,Bulk.FileInfo>>> futures = new ArrayList<>();

    for (FileStatus fileStatus : files) {
      CompletableFuture<Map<KeyExtent,Bulk.FileInfo>> future = CompletableFuture.supplyAsync(() -> {
        try {
          long t1 = System.currentTimeMillis();
          List<KeyExtent> extents = findOverlappingTablets(context, extentCache,
              fileStatus.getPath(), fs);
          Map<KeyExtent,Long> estSizes = estimateSizes(context.getConfiguration(),
              fileStatus.getPath(), fileStatus.getLen(), extents, fs);
          Map<KeyExtent,Bulk.FileInfo> pathLocations = new HashMap<>();
          for (KeyExtent ke : extents) {
            pathLocations.put(ke,
                new Bulk.FileInfo(fileStatus.getPath(), estSizes.getOrDefault(ke, 0L)));
          }
          long t2 = System.currentTimeMillis();
          log.trace("Mapped {} to {} tablets in {}ms", fileStatus.getPath(), pathLocations.size(),
              t2 - t1);
          return pathLocations;
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      }, executor);

      futures.add(future);
    }

    SortedMap<KeyExtent,Bulk.Files> mappings = new TreeMap<>();

    for (CompletableFuture<Map<KeyExtent,Bulk.FileInfo>> future : futures) {
      try {
        Map<KeyExtent,Bulk.FileInfo> pathMapping = future.get();
        pathMapping.forEach((extent, path) -> {
          mappings.computeIfAbsent(extent, k -> new Bulk.Files()).add(path);
        });
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    return mergeOverlapping(mappings);
  }

  // This method handles the case of splits happening while files are being examined. It merges
  // smaller tablets into large tablets.
  static SortedMap<KeyExtent,Bulk.Files> mergeOverlapping(
      SortedMap<KeyExtent,Bulk.Files> mappings) {
    List<KeyExtent> extents = new ArrayList<>(mappings.keySet());

    for (KeyExtent ke : extents) {
      Set<KeyExtent> overlapping = KeyExtent.findOverlapping(ke, mappings);
      for (KeyExtent oke : overlapping) {
        if (ke.equals(oke)) {
          continue;
        }

        boolean containsPrevRow = ke.getPrevEndRow() == null || (oke.getPrevEndRow() != null
            && ke.getPrevEndRow().compareTo(oke.getPrevEndRow()) <= 0);
        boolean containsEndRow = ke.getEndRow() == null
            || (oke.getEndRow() != null && ke.getEndRow().compareTo(oke.getEndRow()) >= 0);

        if (containsPrevRow && containsEndRow) {
          mappings.get(ke).merge(mappings.remove(oke));
        } else {
          throw new RuntimeException("TODO handle merges");
        }
      }
    }

    return mappings;
  }

  private String doFateOperation(FateOperation op, List<ByteBuffer> args, Map<String,String> opts,
      String tableName) throws AccumuloSecurityException, AccumuloException {
    try {
      return new TableOperationsImpl(context).doFateOperation(op, args, opts, tableName);
    } catch (TableExistsException | TableNotFoundException | NamespaceNotFoundException
        | NamespaceExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }
}
