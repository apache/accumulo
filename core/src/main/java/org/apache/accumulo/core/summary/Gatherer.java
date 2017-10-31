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

package org.apache.accumulo.core.summary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ServerClient;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.TRowRange;
import org.apache.accumulo.core.data.thrift.TSummaries;
import org.apache.accumulo.core.data.thrift.TSummaryRequest;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.metadata.schema.MetadataScanner;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.CancelFlagFuture;
import org.apache.accumulo.core.util.CompletableFutureUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

/**
 * This class implements using multiple tservers to gather summaries.
 *
 * Below is a rough outline of the RPC process.
 *
 * <ol>
 * <li>Clients pick a random tserver and make an RPC to remotely execute {@link #gather(ExecutorService)}.
 * <li> {@link #gather(ExecutorService)} will call make RPC calls to multiple tservers to remotely execute {@link #processPartition(ExecutorService, int, int)}
 * <li> {@link #processPartition(ExecutorService, int, int)} will make RPC calls to multiple tserver to remotely execute
 * <li> {@link #processFiles(FileSystemResolver, Map, BlockCache, BlockCache, ExecutorService)}
 * </ol>
 */
public class Gatherer {

  private static final Logger log = LoggerFactory.getLogger(Gatherer.class);

  private ClientContext ctx;
  private Table.ID tableId;
  private SummarizerFactory factory;
  private Text startRow = null;
  private Text endRow = null;
  private Range clipRange;
  private Predicate<SummarizerConfiguration> summarySelector;

  private TSummaryRequest request;

  private String summarizerPattern;

  private Set<SummarizerConfiguration> summaries;

  public Gatherer(ClientContext context, TSummaryRequest request, AccumuloConfiguration tableConfig) {
    this.ctx = context;
    this.tableId = Table.ID.of(request.tableId);
    this.startRow = ByteBufferUtil.toText(request.bounds.startRow);
    this.endRow = ByteBufferUtil.toText(request.bounds.endRow);
    this.clipRange = new Range(startRow, false, endRow, true);
    this.summaries = request.getSummarizers().stream().map(SummarizerConfigurationUtil::fromThrift).collect(Collectors.toSet());
    this.request = request;

    this.summarizerPattern = request.getSummarizerPattern();

    if (summarizerPattern != null) {
      Pattern pattern = Pattern.compile(summarizerPattern);
      // The way conf is converted to string below is documented in the API, so consider this when making changes!
      summarySelector = conf -> pattern.matcher(conf.getClassName() + " " + new TreeMap<>(conf.getOptions())).matches();
      if (!summaries.isEmpty()) {
        summarySelector = summarySelector.or(conf -> summaries.contains(conf));
      }
    } else if (!summaries.isEmpty()) {
      summarySelector = conf -> summaries.contains(conf);
    } else {
      summarySelector = conf -> true;
    }

    this.factory = new SummarizerFactory(tableConfig);
  }

  private TSummaryRequest getRequest() {
    return request;
  }

  /**
   * @param fileSelector
   *          only returns files that match this predicate
   * @return A map of the form : {@code map<tserver location, map<path, list<range>>} . The ranges associated with a file represent the tablets that use the
   *         file.
   */
  private Map<String,Map<String,List<TRowRange>>> getFilesGroupedByLocation(Predicate<String> fileSelector) throws TableNotFoundException, AccumuloException,
      AccumuloSecurityException {

    Iterable<TabletMetadata> tmi = MetadataScanner.builder().from(ctx).overUserTableId(tableId, startRow, endRow).fetchFiles().fetchLocation().fetchLast()
        .fetchPrev().build();

    // get a subset of files
    Map<String,List<TabletMetadata>> files = new HashMap<>();
    for (TabletMetadata tm : tmi) {
      for (String file : tm.getFiles()) {
        if (fileSelector.test(file)) {
          // TODO push this filtering to server side and possibly use batch scanner
          files.computeIfAbsent(file, s -> new ArrayList<>()).add(tm);
        }
      }
    }

    // group by location, then file

    Map<String,Map<String,List<TRowRange>>> locations = new HashMap<>();

    List<String> tservers = null;

    for (Entry<String,List<TabletMetadata>> entry : files.entrySet()) {

      String location = entry.getValue().stream().filter(tm -> tm.getLocation() != null) // filter tablets w/o a location
          .map(tm -> tm.getLocation().getHostAndPort().toString()) // convert to host:port strings
          .min(String::compareTo) // find minimum host:port
          .orElse(entry.getValue().stream().filter(tm -> tm.getLast() != null) // if no locations, then look at last locations
              .map(tm -> tm.getLast().getHostAndPort().toString()) // convert to host:port strings
              .min(String::compareTo).orElse(null)); // find minimum last location or return null

      if (location == null) {
        if (tservers == null) {
          tservers = ctx.getConnector().instanceOperations().getTabletServers();
          Collections.sort(tservers);
        }

        // When no location, the approach below will consistently choose the same tserver for the same file (as long as the set of tservers is stable).
        int idx = Math.abs(Hashing.murmur3_32().hashString(entry.getKey()).asInt()) % tservers.size();
        location = tservers.get(idx);
      }

      List<Range> merged = Range.mergeOverlapping(Lists.transform(entry.getValue(), tm -> tm.getExtent().toDataRange())); // merge contiguous ranges
      List<TRowRange> ranges = merged.stream().map(r -> toClippedExtent(r).toThrift()).collect(Collectors.toList()); // clip ranges to queried range

      locations.computeIfAbsent(location, s -> new HashMap<>()).put(entry.getKey(), ranges);
    }

    return locations;
  }

  private <K,V> Iterable<Map<K,V>> partition(Map<K,V> map, int max) {

    if (map.size() < max) {
      return Collections.singletonList(map);
    }

    return new Iterable<Map<K,V>>() {
      @Override
      public Iterator<Map<K,V>> iterator() {
        Iterator<Entry<K,V>> esi = map.entrySet().iterator();

        return new Iterator<Map<K,V>>() {
          @Override
          public boolean hasNext() {
            return esi.hasNext();
          }

          @Override
          public Map<K,V> next() {
            Map<K,V> workingMap = new HashMap<>(max);
            while (esi.hasNext() && workingMap.size() < max) {
              Entry<K,V> entry = esi.next();
              workingMap.put(entry.getKey(), entry.getValue());
            }
            return workingMap;
          }
        };
      }
    };
  }

  private static class ProcessedFiles {
    final SummaryCollection summaries;
    final Set<String> failedFiles;

    public ProcessedFiles() {
      this.summaries = new SummaryCollection();
      this.failedFiles = new HashSet<>();
    }

    public ProcessedFiles(SummaryCollection summaries, SummarizerFactory factory) {
      this();
      this.summaries.merge(summaries, factory);
    }

    static ProcessedFiles merge(ProcessedFiles pf1, ProcessedFiles pf2, SummarizerFactory factory) {
      ProcessedFiles ret = new ProcessedFiles();
      ret.failedFiles.addAll(pf1.failedFiles);
      ret.failedFiles.addAll(pf2.failedFiles);
      ret.summaries.merge(pf1.summaries, factory);
      ret.summaries.merge(pf2.summaries, factory);
      return ret;
    }
  }

  private class FilesProcessor implements Supplier<ProcessedFiles> {

    HostAndPort location;
    Map<String,List<TRowRange>> allFiles;
    private TInfo tinfo;
    private AtomicBoolean cancelFlag;

    public FilesProcessor(TInfo tinfo, HostAndPort location, Map<String,List<TRowRange>> allFiles, AtomicBoolean cancelFlag) {
      this.location = location;
      this.allFiles = allFiles;
      this.tinfo = tinfo;
      this.cancelFlag = cancelFlag;
    }

    @Override
    public ProcessedFiles get() {
      ProcessedFiles pfiles = new ProcessedFiles();

      Client client = null;
      try {
        client = ThriftUtil.getTServerClient(location, ctx);
        // partition files into smaller chunks so that not too many are sent to a tserver at once
        for (Map<String,List<TRowRange>> files : partition(allFiles, 500)) {
          if (pfiles.failedFiles.size() > 0) {
            // there was a previous failure on this tserver, so just fail the rest of the files
            pfiles.failedFiles.addAll(files.keySet());
            continue;
          }

          try {
            TSummaries tSums = client.startGetSummariesFromFiles(tinfo, ctx.rpcCreds(), getRequest(), files);
            while (!tSums.finished && !cancelFlag.get()) {
              tSums = client.contiuneGetSummaries(tinfo, tSums.sessionId);
            }

            pfiles.summaries.merge(new SummaryCollection(tSums), factory);
          } catch (TApplicationException tae) {
            throw new RuntimeException(tae);
          } catch (TTransportException e) {
            pfiles.failedFiles.addAll(files.keySet());
            continue;
          } catch (TException e) {
            throw new RuntimeException(e);
          }

        }
      } catch (TTransportException e1) {
        pfiles.failedFiles.addAll(allFiles.keySet());
      } finally {
        ThriftUtil.returnClient(client);
      }

      if (cancelFlag.get()) {
        throw new RuntimeException("Operation canceled");
      }

      return pfiles;
    }
  }

  private class PartitionFuture implements Future<SummaryCollection> {

    private CompletableFuture<ProcessedFiles> future;
    private int modulus;
    private int remainder;
    private ExecutorService execSrv;
    private TInfo tinfo;
    private AtomicBoolean cancelFlag = new AtomicBoolean(false);

    PartitionFuture(TInfo tinfo, ExecutorService execSrv, int modulus, int remainder) {
      this.tinfo = tinfo;
      this.execSrv = execSrv;
      this.modulus = modulus;
      this.remainder = remainder;
    }

    private synchronized void initiateProcessing(ProcessedFiles previousWork) {
      try {
        Predicate<String> fileSelector = file -> Math.abs(Hashing.murmur3_32().hashString(file).asInt()) % modulus == remainder;
        if (previousWork != null) {
          fileSelector = fileSelector.and(file -> previousWork.failedFiles.contains(file));
        }
        Map<String,Map<String,List<TRowRange>>> filesGBL;
        filesGBL = getFilesGroupedByLocation(fileSelector);

        List<CompletableFuture<ProcessedFiles>> futures = new ArrayList<>();
        if (previousWork != null) {
          futures.add(CompletableFuture.completedFuture(new ProcessedFiles(previousWork.summaries, factory)));
        }

        for (Entry<String,Map<String,List<TRowRange>>> entry : filesGBL.entrySet()) {
          HostAndPort location = HostAndPort.fromString(entry.getKey());
          Map<String,List<TRowRange>> allFiles = entry.getValue();

          futures.add(CompletableFuture.supplyAsync(new FilesProcessor(tinfo, location, allFiles, cancelFlag), execSrv));
        }

        future = CompletableFutureUtil.merge(futures, (pf1, pf2) -> ProcessedFiles.merge(pf1, pf2, factory), ProcessedFiles::new);

        // when all processing is done, check for failed files... and if found starting processing again
        future.thenRun(() -> updateFuture());
      } catch (Exception e) {
        future = CompletableFuture.completedFuture(new ProcessedFiles());
        // force future to have this exception
        future.obtrudeException(e);
      }
    }

    private ProcessedFiles _get() {
      try {
        return future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    private synchronized CompletableFuture<ProcessedFiles> updateFuture() {
      if (future.isDone()) {
        if (!future.isCancelled() && !future.isCompletedExceptionally()) {
          ProcessedFiles pf = _get();
          if (pf.failedFiles.size() > 0) {
            initiateProcessing(pf);
          }
        }
      }

      return future;
    }

    synchronized void initiateProcessing() {
      Preconditions.checkState(future == null);
      initiateProcessing(null);
    }

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
      boolean canceled = future.cancel(mayInterruptIfRunning);
      if (canceled) {
        cancelFlag.set(true);
      }
      return canceled;
    }

    @Override
    public synchronized boolean isCancelled() {
      return future.isCancelled();
    }

    @Override
    public synchronized boolean isDone() {
      updateFuture();
      if (future.isDone()) {
        if (future.isCancelled() || future.isCompletedExceptionally()) {
          return true;
        }

        ProcessedFiles pf = _get();
        if (pf.failedFiles.size() == 0) {
          return true;
        } else {
          updateFuture();
        }
      }

      return false;
    }

    @Override
    public SummaryCollection get() throws InterruptedException, ExecutionException {
      CompletableFuture<ProcessedFiles> futureRef = updateFuture();
      ProcessedFiles processedFiles = futureRef.get();
      while (processedFiles.failedFiles.size() > 0) {
        futureRef = updateFuture();
        processedFiles = futureRef.get();
      }
      return processedFiles.summaries;
    }

    @Override
    public SummaryCollection get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      long nanosLeft = unit.toNanos(timeout);
      long t1, t2;
      CompletableFuture<ProcessedFiles> futureRef = updateFuture();
      t1 = System.nanoTime();
      ProcessedFiles processedFiles = futureRef.get(Long.max(1, nanosLeft), TimeUnit.NANOSECONDS);
      t2 = System.nanoTime();
      nanosLeft -= (t2 - t1);
      while (processedFiles.failedFiles.size() > 0) {
        futureRef = updateFuture();
        t1 = System.nanoTime();
        processedFiles = futureRef.get(Long.max(1, nanosLeft), TimeUnit.NANOSECONDS);
        t2 = System.nanoTime();
        nanosLeft -= (t2 - t1);
      }
      return processedFiles.summaries;
    }

  }

  /**
   * This methods reads a subset of file paths into memory and groups them by location. Then it request sumaries for files from each location/tablet server.
   */
  public Future<SummaryCollection> processPartition(ExecutorService execSrv, int modulus, int remainder) {
    PartitionFuture future = new PartitionFuture(Tracer.traceInfo(), execSrv, modulus, remainder);
    future.initiateProcessing();
    return future;
  }

  public static interface FileSystemResolver {
    FileSystem get(Path file);
  }

  /**
   * This method will read summaries from a set of files.
   */
  public Future<SummaryCollection> processFiles(FileSystemResolver volMgr, Map<String,List<TRowRange>> files, BlockCache summaryCache, BlockCache indexCache,
      ExecutorService srp) {
    List<CompletableFuture<SummaryCollection>> futures = new ArrayList<>();
    for (Entry<String,List<TRowRange>> entry : files.entrySet()) {
      futures.add(CompletableFuture.supplyAsync(() -> {
        List<RowRange> rrl = Lists.transform(entry.getValue(), RowRange::new);
        return getSummaries(volMgr, entry.getKey(), rrl, summaryCache, indexCache);
      }, srp));
    }

    return CompletableFutureUtil.merge(futures, (sc1, sc2) -> SummaryCollection.merge(sc1, sc2, factory), SummaryCollection::new);
  }

  private int countFiles() throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    // TODO use a batch scanner + iterator to parallelize counting files
    Iterable<TabletMetadata> tmi = MetadataScanner.builder().from(ctx).overUserTableId(tableId, startRow, endRow).fetchFiles().fetchPrev().build();
    return StreamSupport.stream(tmi.spliterator(), false).mapToInt(tm -> tm.getFiles().size()).sum();
  }

  private class GatherRequest implements Supplier<SummaryCollection> {

    private int remainder;
    private int modulus;
    private TInfo tinfo;
    private AtomicBoolean cancelFlag;

    GatherRequest(TInfo tinfo, int remainder, int modulus, AtomicBoolean cancelFlag) {
      this.remainder = remainder;
      this.modulus = modulus;
      this.tinfo = tinfo;
      this.cancelFlag = cancelFlag;
    }

    @Override
    public SummaryCollection get() {
      TSummaryRequest req = getRequest();

      TSummaries tSums;
      try {
        tSums = ServerClient.execute(ctx, new TabletClientService.Client.Factory(), client -> {
          TSummaries tsr = client.startGetSummariesForPartition(tinfo, ctx.rpcCreds(), req, modulus, remainder);
          while (!tsr.finished && !cancelFlag.get()) {
            tsr = client.contiuneGetSummaries(tinfo, tsr.sessionId);
          }
          return tsr;
        });
      } catch (AccumuloException | AccumuloSecurityException e) {
        throw new RuntimeException(e);
      }

      if (cancelFlag.get()) {
        throw new RuntimeException("Operation canceled");
      }

      return new SummaryCollection(tSums);
    }
  }

  public Future<SummaryCollection> gather(ExecutorService es) {
    int numFiles;
    try {
      numFiles = countFiles();
    } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }

    log.debug("Gathering summaries from {} files", numFiles);

    if (numFiles == 0) {
      return CompletableFuture.completedFuture(new SummaryCollection());
    }

    // have each tablet server process ~100K files
    int numRequest = Math.max(numFiles / 100_000, 1);

    List<CompletableFuture<SummaryCollection>> futures = new ArrayList<>();

    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    TInfo tinfo = Tracer.traceInfo();
    for (int i = 0; i < numRequest; i++) {
      futures.add(CompletableFuture.supplyAsync(new GatherRequest(tinfo, i, numRequest, cancelFlag), es));
    }

    Future<SummaryCollection> future = CompletableFutureUtil.merge(futures, (sc1, sc2) -> SummaryCollection.merge(sc1, sc2, factory), SummaryCollection::new);
    return new CancelFlagFuture<>(future, cancelFlag);
  }

  private static Text removeTrailingZeroFromRow(Key k) {
    if (k != null) {
      Text t = new Text();
      ByteSequence row = k.getRowData();
      Preconditions.checkArgument(row.length() >= 1 && row.byteAt(row.length() - 1) == 0);
      t.set(row.getBackingArray(), row.offset(), row.length() - 1);
      return t;
    } else {
      return null;
    }
  }

  private RowRange toClippedExtent(Range r) {
    r = clipRange.clip(r);

    Text startRow = removeTrailingZeroFromRow(r.getStartKey());
    Text endRow = removeTrailingZeroFromRow(r.getEndKey());

    return new RowRange(startRow, endRow);
  }

  public static class RowRange {
    private Text startRow;
    private Text endRow;

    public RowRange(KeyExtent ke) {
      this.startRow = ke.getPrevEndRow();
      this.endRow = ke.getEndRow();
    }

    public RowRange(TRowRange trr) {
      this.startRow = ByteBufferUtil.toText(trr.startRow);
      this.endRow = ByteBufferUtil.toText(trr.endRow);
    }

    public RowRange(Text startRow, Text endRow) {
      this.startRow = startRow;
      this.endRow = endRow;
    }

    public Range toRange() {
      return new Range(startRow, false, endRow, true);
    }

    public TRowRange toThrift() {
      return new TRowRange(TextUtil.getByteBuffer(startRow), TextUtil.getByteBuffer(endRow));
    }

    public Text getStartRow() {
      return startRow;
    }

    public Text getEndRow() {
      return endRow;
    }

    public String toString() {
      return startRow + " " + endRow;
    }
  }

  private SummaryCollection getSummaries(FileSystemResolver volMgr, String file, List<RowRange> ranges, BlockCache summaryCache, BlockCache indexCache) {
    Path path = new Path(file);
    Configuration conf = CachedConfiguration.getInstance();
    return SummaryReader.load(volMgr.get(path), conf, ctx.getConfiguration(), factory, path, summarySelector, summaryCache, indexCache).getSummaries(ranges);
  }
}
