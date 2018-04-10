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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations.ImportExecutorOptions;
import org.apache.accumulo.core.client.admin.TableOperations.ImportSourceArguments;
import org.apache.accumulo.core.client.admin.TableOperations.ImportSourceOptions;
import org.apache.accumulo.core.client.impl.Table.ID;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataScanner;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

public class BulkImport implements ImportSourceArguments, ImportExecutorOptions {

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

    Path srcPath = new Path(dir);

    Executor executor;
    ExecutorService service = null;

    if (this.executor != null) {
      executor = this.executor;
    } else if (numThreads > 0) {
      executor = service = Executors.newFixedThreadPool(numThreads);
    } else {
      String threads = context.getConfiguration().get(ClientProperty.BULK_LOAD_THREADS.getKey());
      if (threads == null) {
        threads = ClientProperty.BULK_LOAD_THREADS.getDefaultValue();
      }

      int nThreads;
      if (threads.toUpperCase().endsWith("C")) {
        nThreads = Runtime.getRuntime().availableProcessors()
            * Integer.parseInt(threads.substring(0, threads.length() - 1));
      } else {
        nThreads = Integer.parseInt(threads);
      }
      executor = service = Executors.newFixedThreadPool(nThreads);
    }

    try {
      SortedMap<KeyExtent,Files> mappings = BulkImport.computeFileToTabletMappings(tableId, srcPath,
          executor, context);

      // TODO remove this code, master needs to do this
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(srcPath.toUri(), conf);
      Path dirPath = new Path(srcPath.getParent().getParent(),
          "accumulo/tables/" + tableId + "/b-214999");

      Map<String,String> renames = new HashMap<>();

      int count = 1 << 22;
      for (FileStatus status : fs.listStatus(srcPath)) {
        Path newName = new Path(dirPath, "I" + Integer.toString(count++, 36) + ".rf");
        System.out.println("rename " + status.getPath() + " " + newName);
        fs.rename(status.getPath(), newName);
        renames.put(status.getPath().getName(), newName.getName());
      }

      mappings = mapNames(mappings, renames);

      context.getConnector().tableOperations().offline(tableName, true);

      BulkImport.writeFilesToMetadata(tableId, "file:" + dirPath.toString(), mappings, context);

      context.getConnector().tableOperations().online(tableName, true);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e);
    } finally {
      if (service != null) {
        service.shutdown();
        // TODO wait
      }
    }
  }

  private SortedMap<KeyExtent,Files> mapNames(SortedMap<KeyExtent,Files> mappings,
      Map<String,String> renames) {
    SortedMap<KeyExtent,Files> newMappings = new TreeMap<>();

    mappings.forEach((k, v) -> {
      newMappings.put(k, v.mapNames(renames));
    });

    return newMappings;
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
  public ImportSourceOptions from(String directory) {
    this.dir = Objects.requireNonNull(directory);
    return this;
  }

  // TODO this code opens files multiple times... each time a file is opened the file len is
  // retrieved from NN. This could be done once and cached.

  private final static byte[] byte0 = {0};

  static class FileInfo {
    final String fileName;
    final long estFileSize;
    final long estNumEntries;

    FileInfo(String fileName, long estFileSize, long estNumEntries) {
      this.fileName = fileName;
      this.estFileSize = estFileSize;
      this.estNumEntries = estNumEntries;
    }

    public FileInfo(Path path, long estSize) {
      this(path.getName(), estSize, 0);
    }

    static FileInfo merge(FileInfo fi1, FileInfo fi2) {
      Preconditions.checkArgument(fi1.fileName.equals(fi2.fileName));
      return new FileInfo(fi1.fileName, fi1.estFileSize + fi2.estFileSize,
          fi1.estNumEntries + fi2.estNumEntries);
    }

    @Override
    public String toString() {
      return String.format("file:%s  estSize:%d estEntries:%s", fileName, estFileSize,
          estNumEntries);
    }
  }

  static class Files implements Iterable<FileInfo> {
    Map<String,FileInfo> files = new HashMap<>();

    void add(FileInfo fi) {
      if (files.putIfAbsent(fi.fileName, fi) != null) {
        throw new IllegalArgumentException("File already present " + fi.fileName);
      }
    }

    public Files mapNames(Map<String,String> renames) {
      Files renamed = new Files();

      files.forEach((k, v) -> {
        String newName = renames.get(k);
        FileInfo nfi = new FileInfo(newName, v.estFileSize, v.estNumEntries);
        renamed.files.put(newName, nfi);
      });

      return renamed;
    }

    void merge(Files other) {
      other.files.forEach((k, v) -> {
        files.merge(k, v, FileInfo::merge);
      });
    }

    @Override
    public Iterator<FileInfo> iterator() {
      return files.values().iterator();
    }
  }

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
        // continue with next file
        // TODO this was logging
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

  public static List<TabletLocation> findOverlappingTablets(ClientContext context,
      TabletLocator locator, Text startRow, Text endRow, FileSKVIterator reader)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    List<TabletLocation> result = new ArrayList<>();
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
      TabletLocation tabletLocation = locator.locateTablet(context, row, false, true);
      // log.debug(filename + " found row " + row + " at location " + tabletLocation);
      result.add(tabletLocation);
      row = tabletLocation.tablet_extent.getEndRow();
      if (row != null && (endRow == null || row.compareTo(endRow) < 0)) {
        row = new Text(row);
        row.append(byte0, 0, byte0.length);
      } else
        break;
    }

    return result;
  }

  public static List<TabletLocation> findOverlappingTablets(ClientContext context,
      TabletLocator locator, Path file, FileSystem fs)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    try (FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder()
        .forFile(file.toString(), fs, fs.getConf())
        .withTableConfiguration(context.getConfiguration()).seekToBeginning().build()) {
      return findOverlappingTablets(context, locator, null, null, reader);
    }
  }

  public static SortedMap<KeyExtent,Files> computeFileToTabletMappings(Table.ID tableId,
      Path dirPath, Executor executor, ClientContext context)
      throws IOException, URISyntaxException {
    // TODO is there a standard way to get Hadoop config on client side

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(dirPath.toUri(), conf);

    TabletLocator locator = TabletLocator.getLocator(context, tableId);

    // TODO see how current code filters files in dir
    FileStatus[] files = fs.listStatus(dirPath, p -> p.getName().endsWith(".rf"));

    List<CompletableFuture<Map<KeyExtent,FileInfo>>> futures = new ArrayList<>();

    for (FileStatus fileStatus : files) {
      CompletableFuture<Map<KeyExtent,FileInfo>> future = CompletableFuture.supplyAsync(() -> {
        try {
          List<TabletLocation> locations = findOverlappingTablets(context, locator,
              fileStatus.getPath(), fs);
          Collection<KeyExtent> extents = Collections2.transform(locations, l -> l.tablet_extent);
          Map<KeyExtent,Long> estSizes = estimateSizes(context.getConfiguration(),
              fileStatus.getPath(), fileStatus.getLen(), extents, fs);
          Map<KeyExtent,FileInfo> pathLocations = new HashMap<>();
          for (TabletLocation location : locations) {
            KeyExtent ke = location.tablet_extent;
            pathLocations.put(ke,
                new FileInfo(fileStatus.getPath(), estSizes.getOrDefault(ke, 0L)));
          }
          return pathLocations;
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      }, executor);

      futures.add(future);
    }

    SortedMap<KeyExtent,Files> mappings = new TreeMap<>();

    for (CompletableFuture<Map<KeyExtent,FileInfo>> future : futures) {
      try {
        Map<KeyExtent,FileInfo> pathMapping = future.get();
        pathMapping.forEach((extent, path) -> {
          mappings.computeIfAbsent(extent, k -> new Files()).add(path);
        });
      } catch (InterruptedException e) {
        // TODO is this ok
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        // TODO better exception handling
        throw new RuntimeException(e);
      }
    }
    return mergeOverlapping(mappings);
  }

  // This method handles the case of splits happening while files are being examined. It merges
  // smaller tablets into large tablets.
  static SortedMap<KeyExtent,Files> mergeOverlapping(SortedMap<KeyExtent,Files> mappings) {
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
          // TODO evidence of a merge
        }
      }
    }

    return mappings;
  }

  private static boolean equals(Text t1, Text t2) {
    if (t1 == null || t2 == null)
      return t1 == t2;

    return t1.equals(t2);
  }

  public static void writeFilesToMetadata(ID tableId, String dirUri,
      SortedMap<KeyExtent,Files> mappings, ClientContext context)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    // TODO restrict range of scan based on min and max rows in key extents.
    Iterable<TabletMetadata> tableMetadata = MetadataScanner.builder().from(context)
        .overUserTableId(tableId).fetchPrev().build();

    Iterator<TabletMetadata> tabletIter = tableMetadata.iterator();
    Iterator<Entry<KeyExtent,Files>> fileIterator = mappings.entrySet().iterator();

    List<TabletMetadata> tablets = new ArrayList<>();

    context.getConnector().securityOperations().grantTablePermission("root", MetadataTable.NAME,
        TablePermission.WRITE);
    BatchWriter bw = context.getConnector().createBatchWriter(MetadataTable.NAME);

    TabletMetadata currentTablet = tabletIter.next();
    while (fileIterator.hasNext()) {
      Entry<KeyExtent,Files> fileEntry = fileIterator.next();

      KeyExtent fke = fileEntry.getKey();

      tablets.clear();

      while (!equals(currentTablet.getPrevEndRow(), fke.getPrevEndRow())) {
        currentTablet = tabletIter.next();
      }

      tablets.add(currentTablet);

      while (!equals(currentTablet.getEndRow(), fke.getEndRow())) {
        currentTablet = tabletIter.next();
        tablets.add(currentTablet);
      }

      for (TabletMetadata tablet : tablets) {

        Mutation mutation = new Mutation(tablet.getExtent().getMetadataEntry());

        Files files = fileEntry.getValue();
        for (FileInfo fileInfo : files) {
          long estSize = (long) (fileInfo.estFileSize / (double) tablets.size());
          System.out.println("Add file " + fileInfo.fileName + " to " + tablet.getExtent() + " est "
              + estSize + " orig est " + fileInfo.estFileSize);

          String fileFamily = MetadataSchema.TabletsSection.DataFileColumnFamily.NAME.toString();
          System.out.println("put : " + dirUri + "/" + fileInfo.fileName);
          mutation.put(fileFamily, dirUri + "/" + fileInfo.fileName,
              fileInfo.estFileSize + "," + fileInfo.estNumEntries);
        }
        bw.addMutation(mutation);
      }
    }

    bw.close();
  }
}
