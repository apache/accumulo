/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.clientImpl.bulk;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.groupingBy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations.ImportDestinationArguments;
import org.apache.accumulo.core.client.admin.TableOperations.ImportMappingOptions;
import org.apache.accumulo.core.clientImpl.AccumuloBulkMergeException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.bulk.Bulk.FileInfo;
import org.apache.accumulo.core.clientImpl.bulk.Bulk.Files;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.LoadPlan;
import org.apache.accumulo.core.data.LoadPlan.Destination;
import org.apache.accumulo.core.data.LoadPlan.RangeType;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.fate.util.Retry;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;

public class BulkImport implements ImportDestinationArguments, ImportMappingOptions {

  private static final Logger log = LoggerFactory.getLogger(BulkImport.class);

  private boolean setTime = false;
  private Executor executor = null;
  private final String dir;
  private int numThreads = -1;

  private final ClientContext context;
  private String tableName;

  private LoadPlan plan = null;

  public BulkImport(String directory, ClientContext context) {
    this.context = context;
    this.dir = Objects.requireNonNull(directory);
  }

  @Override
  public ImportMappingOptions tableTime(boolean value) {
    this.setTime = value;
    return this;
  }

  @Override
  public void load()
      throws TableNotFoundException, IOException, AccumuloException, AccumuloSecurityException {

    TableId tableId = Tables.getTableId(context, tableName);

    Map<String,String> props = context.instanceOperations().getSystemConfiguration();
    AccumuloConfiguration conf = new ConfigurationCopy(props);

    FileSystem fs =
        VolumeConfiguration.getVolume(dir, context.getHadoopConf(), conf).getFileSystem();

    Path srcPath = checkPath(fs, dir);

    SortedMap<KeyExtent,Bulk.Files> mappings;
    TableOperationsImpl tableOps = new TableOperationsImpl(context);
    Retry retry = Retry.builder().infiniteRetries().retryAfter(100, MILLISECONDS)
        .incrementBy(100, MILLISECONDS).maxWait(2, MINUTES).backOffFactor(1.5)
        .logInterval(3, TimeUnit.MINUTES).createRetry();

    // retry if a merge occurs
    boolean shouldRetry = true;
    while (shouldRetry) {
      if (plan == null) {
        mappings = computeMappingFromFiles(fs, tableId, srcPath);
      } else {
        mappings = computeMappingFromPlan(fs, tableId, srcPath);
      }

      if (mappings.isEmpty())
        throw new IllegalArgumentException("Attempted to import zero files from " + srcPath);

      BulkSerialize.writeLoadMapping(mappings, srcPath.toString(), fs::create);

      List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableId.canonical().getBytes(UTF_8)),
          ByteBuffer.wrap(srcPath.toString().getBytes(UTF_8)),
          ByteBuffer.wrap((setTime + "").getBytes(UTF_8)));
      try {
        tableOps.doBulkFateOperation(args, tableName);
        shouldRetry = false;
      } catch (AccumuloBulkMergeException ae) {
        if (plan != null) {
          checkPlanForSplits(ae);
        }
        try {
          retry.waitForNextAttempt();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        log.info(ae.getMessage() + ". Retrying bulk import to " + tableName);
      }
    }
  }

  /**
   * Check if splits were specified in plan when a concurrent merge occurred. If so, throw error
   * back to user since retrying won't help. If not, then retry.
   */
  private void checkPlanForSplits(AccumuloBulkMergeException abme) throws AccumuloException {
    for (Destination des : plan.getDestinations()) {
      if (des.getRangeType().equals(RangeType.TABLE)) {
        throw new AccumuloException("The splits provided in Load Plan do not exist in " + tableName,
            abme);
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

    // TODO ensure dir does not contain bulk load mapping

    return ret;
  }

  @Override
  public ImportMappingOptions executor(Executor service) {
    this.executor = Objects.requireNonNull(service);
    return this;
  }

  @Override
  public ImportMappingOptions threads(int numThreads) {
    Preconditions.checkArgument(numThreads > 0, "Non positive number of threads given : %s",
        numThreads);
    this.numThreads = numThreads;
    return this;
  }

  @Override
  public ImportMappingOptions plan(LoadPlan plan) {
    this.plan = Objects.requireNonNull(plan);
    return this;
  }

  @Override
  public ImportMappingOptions to(String tableName) {
    this.tableName = Objects.requireNonNull(tableName);
    return this;
  }

  private static final byte[] byte0 = {0};

  private static class MLong {
    public MLong(long i) {
      l = i;
    }

    long l;
  }

  public static Map<KeyExtent,Long> estimateSizes(AccumuloConfiguration acuConf, Path mapFile,
      long fileSize, Collection<KeyExtent> extents, FileSystem ns, Cache<String,Long> fileLenCache,
      CryptoService cs) throws IOException {

    if (extents.size() == 1) {
      return Collections.singletonMap(extents.iterator().next(), fileSize);
    }

    long totalIndexEntries = 0;
    Map<KeyExtent,MLong> counts = new TreeMap<>();
    for (KeyExtent keyExtent : extents)
      counts.put(keyExtent, new MLong(0));

    Text row = new Text();

    FileSKVIterator index = FileOperations.getInstance().newIndexReaderBuilder()
        .forFile(mapFile.toString(), ns, ns.getConf(), cs).withTableConfiguration(acuConf)
        .withFileLenCache(fileLenCache).build();

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
        throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
  }

  public static List<KeyExtent> findOverlappingTablets(KeyExtentCache extentCache,
      FileSKVIterator reader)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {

    Text startRow = null;
    Text endRow = null;

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
        row = nextRow(row);
      } else
        break;
    }

    return result;
  }

  private static Text nextRow(Text row) {
    Text next = new Text(row);
    next.append(byte0, 0, byte0.length);
    return next;
  }

  public static List<KeyExtent> findOverlappingTablets(ClientContext context,
      KeyExtentCache extentCache, Path file, FileSystem fs, Cache<String,Long> fileLenCache,
      CryptoService cs)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    try (FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder()
        .forFile(file.toString(), fs, fs.getConf(), cs)
        .withTableConfiguration(context.getConfiguration()).withFileLenCache(fileLenCache)
        .seekToBeginning().build()) {
      return findOverlappingTablets(extentCache, reader);
    }
  }

  private static Map<String,Long> getFileLenMap(List<FileStatus> statuses) {
    HashMap<String,Long> fileLens = new HashMap<>();
    for (FileStatus status : statuses) {
      fileLens.put(status.getPath().getName(), status.getLen());
      status.getLen();
    }

    return fileLens;
  }

  private static Cache<String,Long> getPopulatedFileLenCache(Path dir, List<FileStatus> statuses) {
    Map<String,Long> fileLens = getFileLenMap(statuses);

    Map<String,Long> absFileLens = new HashMap<>();
    fileLens.forEach((k, v) -> {
      absFileLens.put(CachableBlockFile.pathToCacheId(new Path(dir, k)), v);
    });

    Cache<String,Long> fileLenCache = CacheBuilder.newBuilder().build();

    fileLenCache.putAll(absFileLens);

    return fileLenCache;
  }

  private SortedMap<KeyExtent,Files> computeMappingFromPlan(FileSystem fs, TableId tableId,
      Path srcPath)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {

    Map<String,List<Destination>> fileDestinations =
        plan.getDestinations().stream().collect(groupingBy(Destination::getFileName));

    List<FileStatus> statuses = filterInvalid(
        fs.listStatus(srcPath, p -> !p.getName().equals(Constants.BULK_LOAD_MAPPING)));

    Map<String,Long> fileLens = getFileLenMap(statuses);

    if (!fileDestinations.keySet().equals(fileLens.keySet())) {
      throw new IllegalArgumentException(
          "Load plan files differ from directory files, symmetric difference : "
              + Sets.symmetricDifference(fileDestinations.keySet(), fileLens.keySet()));
    }

    KeyExtentCache extentCache = new ConcurrentKeyExtentCache(tableId, context);

    // Pre-populate cache by looking up all end rows in sorted order. Doing this in sorted order
    // leverages read ahead.
    fileDestinations.values().stream().flatMap(List::stream)
        .filter(dest -> dest.getRangeType() == RangeType.FILE)
        .flatMap(dest -> Stream.of(dest.getStartRow(), dest.getEndRow())).filter(row -> row != null)
        .map(Text::new).sorted().distinct().forEach(row -> {
          try {
            extentCache.lookup(row);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    SortedMap<KeyExtent,Files> mapping = new TreeMap<>();

    for (Entry<String,List<Destination>> entry : fileDestinations.entrySet()) {
      String fileName = entry.getKey();
      List<Destination> destinations = entry.getValue();
      Set<KeyExtent> extents = mapDesitnationsToExtents(tableId, extentCache, destinations);

      long estSize = (long) (fileLens.get(fileName) / (double) extents.size());

      for (KeyExtent keyExtent : extents) {
        mapping.computeIfAbsent(keyExtent, k -> new Files())
            .add(new FileInfo(fileName, estSize, 0));
      }
    }

    return mergeOverlapping(mapping);
  }

  private Text toText(byte[] row) {
    return row == null ? null : new Text(row);
  }

  private Set<KeyExtent> mapDesitnationsToExtents(TableId tableId, KeyExtentCache kec,
      List<Destination> destinations)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Set<KeyExtent> extents = new HashSet<>();

    for (Destination dest : destinations) {

      if (dest.getRangeType() == RangeType.TABLE) {
        extents.add(new KeyExtent(tableId, toText(dest.getEndRow()), toText(dest.getStartRow())));
      } else if (dest.getRangeType() == RangeType.FILE) {
        Text startRow = new Text(dest.getStartRow());
        Text endRow = new Text(dest.getEndRow());

        KeyExtent extent = kec.lookup(startRow);

        extents.add(extent);

        while (!extent.contains(endRow) && extent.getEndRow() != null) {
          extent = kec.lookup(nextRow(extent.getEndRow()));
          extents.add(extent);
        }

      } else {
        throw new IllegalStateException();
      }
    }

    return extents;
  }

  private SortedMap<KeyExtent,Bulk.Files> computeMappingFromFiles(FileSystem fs, TableId tableId,
      Path dirPath) throws IOException {

    Executor executor;
    ExecutorService service = null;

    if (this.executor != null) {
      executor = this.executor;
    } else if (numThreads > 0) {
      executor = service = Executors.newFixedThreadPool(numThreads);
    } else {
      String threads = context.getConfiguration().get(ClientProperty.BULK_LOAD_THREADS.getKey());
      executor =
          service = Executors.newFixedThreadPool(ConfigurationTypeHelper.getNumThreads(threads));
    }

    try {
      return computeFileToTabletMappings(fs, tableId, dirPath, executor, context);
    } finally {
      if (service != null) {
        service.shutdown();
      }
    }

  }

  public static List<FileStatus> filterInvalid(FileStatus[] files) {

    ArrayList<FileStatus> fileList = new ArrayList<>(files.length);
    for (FileStatus fileStatus : files) {

      String fname = fileStatus.getPath().getName();

      if (fileStatus.isDirectory()) {
        log.debug("{} is a directory, ignoring.", fileStatus.getPath());
        continue;
      }

      if (FileOperations.getBulkWorkingFiles().contains(fname)) {
        log.debug("{} is an internal working file, ignoring.", fileStatus.getPath());
        continue;
      }

      if (!FileOperations.getValidExtensions().contains(FilenameUtils.getExtension(fname))) {
        log.warn("{} does not have a valid extension, ignoring", fileStatus.getPath());
        continue;
      }

      fileList.add(fileStatus);
    }

    return fileList;
  }

  public static SortedMap<KeyExtent,Bulk.Files> computeFileToTabletMappings(FileSystem fs,
      TableId tableId, Path dirPath, Executor executor, ClientContext context) throws IOException {

    KeyExtentCache extentCache = new ConcurrentKeyExtentCache(tableId, context);

    List<FileStatus> files = filterInvalid(
        fs.listStatus(dirPath, p -> !p.getName().equals(Constants.BULK_LOAD_MAPPING)));

    // we know all of the file lens, so construct a cache and populate it in order to avoid later
    // trips to the namenode
    Cache<String,Long> fileLensCache = getPopulatedFileLenCache(dirPath, files);

    List<CompletableFuture<Map<KeyExtent,Bulk.FileInfo>>> futures = new ArrayList<>();

    CryptoService cs = CryptoServiceFactory.newDefaultInstance();

    for (FileStatus fileStatus : files) {
      CompletableFuture<Map<KeyExtent,Bulk.FileInfo>> future = CompletableFuture.supplyAsync(() -> {
        try {
          long t1 = System.currentTimeMillis();
          List<KeyExtent> extents = findOverlappingTablets(context, extentCache,
              fileStatus.getPath(), fs, fileLensCache, cs);
          Map<KeyExtent,Long> estSizes = estimateSizes(context.getConfiguration(),
              fileStatus.getPath(), fileStatus.getLen(), extents, fs, fileLensCache, cs);
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
  static SortedMap<KeyExtent,Bulk.Files>
      mergeOverlapping(SortedMap<KeyExtent,Bulk.Files> mappings) {
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
}
