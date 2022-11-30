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
package org.apache.accumulo.core.summary;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TRowRange;
import org.apache.accumulo.core.dataImpl.thrift.TSummaries;
import org.apache.accumulo.core.dataImpl.thrift.TSummaryRequest;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.CancelFlagFuture;
import org.apache.accumulo.core.util.CompletableFutureUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.hash.Hashing;

/**
 * This class implements using multiple tservers to gather summaries.
 *
 * Below is a rough outline of the RPC process.
 *
 * <ol>
 * <li>Clients pick a random tserver and make an RPC to remotely execute
 * {@link #gather(ExecutorService)}.
 * <li>{@link #gather(ExecutorService)} will call make RPC calls to multiple tservers to remotely
 * execute {@link #processPartition(ExecutorService, int, int)}
 * <li>{@link #processPartition(ExecutorService, int, int)} will make RPC calls to multiple tserver
 * to remotely execute
 * <li>{@link #processFiles(FileSystemResolver, Map, BlockCache, BlockCache, Cache, ExecutorService)}
 * </ol>
 */
public class Gatherer {

  private static final Logger log = LoggerFactory.getLogger(Gatherer.class);

  private ClientContext ctx;
  private TableId tableId;
  private SummarizerFactory factory;
  private Text startRow = null;
  private Text endRow = null;
  private Range clipRange;
  private Predicate<SummarizerConfiguration> summarySelector;
  private CryptoService cryptoService;

  private TSummaryRequest request;

  private String summarizerPattern;

  private Set<SummarizerConfiguration> summaries;

  public Gatherer(ClientContext context, TSummaryRequest request, AccumuloConfiguration tableConfig,
      CryptoService cryptoService) {
    this.ctx = context;
    this.tableId = TableId.of(request.tableId);
    this.startRow = ByteBufferUtil.toText(request.bounds.startRow);
    this.endRow = ByteBufferUtil.toText(request.bounds.endRow);
    this.clipRange = new Range(startRow, false, endRow, true);
    this.summaries = request.getSummarizers().stream().map(SummarizerConfigurationUtil::fromThrift)
        .collect(Collectors.toSet());
    this.request = request;
    this.cryptoService = cryptoService;

    this.summarizerPattern = request.getSummarizerPattern();

    if (summarizerPattern != null) {
      Pattern pattern = Pattern.compile(summarizerPattern);
      // The way conf is converted to string below is documented in the API, so consider this when
      // making changes!
      summarySelector = conf -> pattern
          .matcher(conf.getClassName() + " " + new TreeMap<>(conf.getOptions())).matches();
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
   * @param fileSelector only returns files that match this predicate
   * @return A map of the form : {@code map<tserver location, map<path, list<range>>} . The ranges
   *         associated with a file represent the tablets that use the file.
   */
  private Map<String,Map<TabletFile,List<TRowRange>>>
      getFilesGroupedByLocation(Predicate<TabletFile> fileSelector) {

    Iterable<TabletMetadata> tmi = TabletsMetadata.builder(ctx).forTable(tableId)
        .overlapping(startRow, endRow).fetch(FILES, LOCATION, LAST, PREV_ROW).build();

    // get a subset of files
    Map<TabletFile,List<TabletMetadata>> files = new HashMap<>();

    for (TabletMetadata tm : tmi) {
      for (TabletFile file : tm.getFiles()) {
        if (fileSelector.test(file)) {
          // TODO push this filtering to server side and possibly use batch scanner
          files.computeIfAbsent(file, s -> new ArrayList<>()).add(tm);
        }
      }
    }

    // group by location, then file

    Map<String,Map<TabletFile,List<TRowRange>>> locations = new HashMap<>();

    List<String> tservers = null;

    for (Entry<TabletFile,List<TabletMetadata>> entry : files.entrySet()) {

      String location = entry.getValue().stream().filter(tm -> tm.getLocation() != null) // filter
                                                                                         // tablets
                                                                                         // w/o a
                                                                                         // location
          .map(tm -> tm.getLocation().getHostPort()) // convert to host:port strings
          .min(String::compareTo) // find minimum host:port
          .orElse(entry.getValue().stream().filter(tm -> tm.getLast() != null) // if no locations,
                                                                               // then look at last
                                                                               // locations
              .map(tm -> tm.getLast().getHostPort()) // convert to host:port strings
              .min(String::compareTo).orElse(null)); // find minimum last location or return null

      if (location == null) {
        if (tservers == null) {
          tservers = ctx.instanceOperations().getTabletServers();
          Collections.sort(tservers);
        }

        // When no location, the approach below will consistently choose the same tserver for the
        // same file (as long as the set of tservers is stable).
        int idx = Math
            .abs(Hashing.murmur3_32_fixed().hashString(entry.getKey().getPathStr(), UTF_8).asInt())
            % tservers.size();
        location = tservers.get(idx);
      }

      // merge contiguous ranges
      List<Range> merged = Range.mergeOverlapping(entry.getValue().stream()
          .map(tm -> tm.getExtent().toDataRange()).collect(Collectors.toList()));
      List<TRowRange> ranges =
          merged.stream().map(r -> toClippedExtent(r).toThrift()).collect(Collectors.toList()); // clip
                                                                                                // ranges
                                                                                                // to
                                                                                                // queried
                                                                                                // range

      locations.computeIfAbsent(location, s -> new HashMap<>()).put(entry.getKey(), ranges);
    }

    return locations;
  }

  private <K,V> Iterable<Map<K,V>> partition(Map<K,V> map, int max) {

    if (map.size() < max) {
      return Collections.singletonList(map);
    }

    return () -> {
      Iterator<Entry<K,V>> esi = map.entrySet().iterator();

      return new Iterator<>() {
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
    };
  }

  private static class ProcessedFiles {
    final SummaryCollection summaries;
    final Set<TabletFile> failedFiles;

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
    Map<TabletFile,List<TRowRange>> allFiles;
    private TInfo tinfo;
    private AtomicBoolean cancelFlag;

    public FilesProcessor(TInfo tinfo, HostAndPort location,
        Map<TabletFile,List<TRowRange>> allFiles, AtomicBoolean cancelFlag) {
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
        client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, location, ctx);
        // partition files into smaller chunks so that not too many are sent to a tserver at once
        for (Map<TabletFile,List<TRowRange>> files : partition(allFiles, 500)) {
          if (!pfiles.failedFiles.isEmpty()) {
            // there was a previous failure on this tserver, so just fail the rest of the files
            pfiles.failedFiles.addAll(files.keySet());
            continue;
          }

          try {
            TSummaries tSums = client.startGetSummariesFromFiles(tinfo, ctx.rpcCreds(),
                getRequest(), files.entrySet().stream().collect(
                    Collectors.toMap(entry -> entry.getKey().getPathStr(), Entry::getValue)));
            while (!tSums.finished && !cancelFlag.get()) {
              tSums = client.contiuneGetSummaries(tinfo, tSums.sessionId);
            }

            pfiles.summaries.merge(new SummaryCollection(tSums), factory);
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
        ThriftUtil.returnClient(client, ctx);
      }

      if (cancelFlag.get()) {
        throw new RuntimeException("Operation canceled");
      }

      return pfiles;
    }
  }

  private class PartitionFuture implements Future<SummaryCollection> {
    private final CompletableFuture<SummaryCollection> future;
    private final AtomicBoolean cancelFlag = new AtomicBoolean(false);

    PartitionFuture(TInfo tinfo, ExecutorService execSrv, int modulus, int remainder) {
      Function<ProcessedFiles,CompletableFuture<ProcessedFiles>> go = previousWork -> {
        Predicate<TabletFile> fileSelector = file -> Math
            .abs(Hashing.murmur3_32_fixed().hashString(file.getPathStr(), UTF_8).asInt()) % modulus
            == remainder;
        if (previousWork != null) {
          fileSelector = fileSelector.and(previousWork.failedFiles::contains);
        }
        Map<String,Map<TabletFile,List<TRowRange>>> filesGBL;
        filesGBL = getFilesGroupedByLocation(fileSelector);

        List<CompletableFuture<ProcessedFiles>> futures = new ArrayList<>();
        if (previousWork != null) {
          futures.add(CompletableFuture
              .completedFuture(new ProcessedFiles(previousWork.summaries, factory)));
        }

        for (Entry<String,Map<TabletFile,List<TRowRange>>> entry : filesGBL.entrySet()) {
          HostAndPort location = HostAndPort.fromString(entry.getKey());
          Map<TabletFile,List<TRowRange>> allFiles = entry.getValue();

          futures.add(CompletableFuture
              .supplyAsync(new FilesProcessor(tinfo, location, allFiles, cancelFlag), execSrv));
        }

        return CompletableFutureUtil.merge(futures,
            (pf1, pf2) -> ProcessedFiles.merge(pf1, pf2, factory), ProcessedFiles::new);
      };
      future = CompletableFutureUtil
          .iterateUntil(go,
              previousWork -> previousWork != null && previousWork.failedFiles.isEmpty(), null)
          .thenApply(pf -> pf.summaries);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean canceled = future.cancel(mayInterruptIfRunning);
      if (canceled) {
        cancelFlag.set(true);
      }
      return canceled;
    }

    @Override
    public boolean isCancelled() {
      return future.isCancelled();
    }

    @Override
    public boolean isDone() {
      return future.isDone();
    }

    @Override
    public SummaryCollection get() throws InterruptedException, ExecutionException {
      return future.get();
    }

    @Override
    public SummaryCollection get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return future.get(timeout, unit);
    }

  }

  /**
   * This methods reads a subset of file paths into memory and groups them by location. Then it
   * request summaries for files from each location/tablet server.
   */
  public Future<SummaryCollection> processPartition(ExecutorService execSrv, int modulus,
      int remainder) {
    return new PartitionFuture(TraceUtil.traceInfo(), execSrv, modulus, remainder);
  }

  public interface FileSystemResolver {
    FileSystem get(Path file);
  }

  /**
   * This method will read summaries from a set of files.
   */
  public Future<SummaryCollection> processFiles(FileSystemResolver volMgr,
      Map<String,List<TRowRange>> files, BlockCache summaryCache, BlockCache indexCache,
      Cache<String,Long> fileLenCache, ExecutorService srp) {
    List<CompletableFuture<SummaryCollection>> futures = new ArrayList<>();
    for (Entry<String,List<TRowRange>> entry : files.entrySet()) {
      futures.add(CompletableFuture.supplyAsync(() -> {
        List<RowRange> rrl =
            entry.getValue().stream().map(RowRange::new).collect(Collectors.toList());
        return getSummaries(volMgr, entry.getKey(), rrl, summaryCache, indexCache, fileLenCache);
      }, srp));
    }

    return CompletableFutureUtil.merge(futures,
        (sc1, sc2) -> SummaryCollection.merge(sc1, sc2, factory), SummaryCollection::new);
  }

  private int countFiles() {
    // TODO use a batch scanner + iterator to parallelize counting files
    return TabletsMetadata.builder(ctx).forTable(tableId).overlapping(startRow, endRow)
        .fetch(FILES, PREV_ROW).build().stream().mapToInt(tm -> tm.getFiles().size()).sum();
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
        tSums = ThriftClientTypes.TABLET_SERVER.execute(ctx, client -> {
          TSummaries tsr =
              client.startGetSummariesForPartition(tinfo, ctx.rpcCreds(), req, modulus, remainder);
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
    int numFiles = countFiles();

    log.debug("Gathering summaries from {} files", numFiles);

    if (numFiles == 0) {
      return CompletableFuture.completedFuture(new SummaryCollection());
    }

    // have each tablet server process ~100K files
    int numRequest = Math.max(numFiles / 100_000, 1);

    List<CompletableFuture<SummaryCollection>> futures = new ArrayList<>();

    AtomicBoolean cancelFlag = new AtomicBoolean(false);

    TInfo tinfo = TraceUtil.traceInfo();
    for (int i = 0; i < numRequest; i++) {
      futures.add(
          CompletableFuture.supplyAsync(new GatherRequest(tinfo, i, numRequest, cancelFlag), es));
    }

    Future<SummaryCollection> future = CompletableFutureUtil.merge(futures,
        (sc1, sc2) -> SummaryCollection.merge(sc1, sc2, factory), SummaryCollection::new);
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
      this.startRow = ke.prevEndRow();
      this.endRow = ke.endRow();
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

    @Override
    public String toString() {
      return startRow + " " + endRow;
    }
  }

  private SummaryCollection getSummaries(FileSystemResolver volMgr, String file,
      List<RowRange> ranges, BlockCache summaryCache, BlockCache indexCache,
      Cache<String,Long> fileLenCache) {
    Path path = new Path(file);
    Configuration conf = ctx.getHadoopConf();
    return SummaryReader.load(volMgr.get(path), conf, factory, path, summarySelector, summaryCache,
        indexCache, fileLenCache, cryptoService).getSummaries(ranges);
  }
}
