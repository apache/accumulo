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

import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ServerClient;
import org.apache.accumulo.core.client.impl.thrift.TRowRange;
import org.apache.accumulo.core.client.impl.thrift.TSummaries;
import org.apache.accumulo.core.client.impl.thrift.TSummaryRequest;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.metadata.schema.MetadataScanner;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.google.common.net.HostAndPort;

/**
 * This class implements using multiple tservers to gather summaries.
 *
 * Below is a rough outline of the RPC process.
 *
 * <ol>
 * <li>Clients pick a random tserver and make an RPC to remotely execute {@link #gather()}.
 * <li> {@link #gather()} will call make RPC calls to multiple tservers to remotely execute {@link #processPartition(int, int)}
 * <li> {@link #processPartition(int, int)} will make RPC calls to multiple tserver to remotely execute
 * {@link #processFiles(FileSystemResolver, Map, BlockCache, BlockCache, ExecutorService)}
 * </ol>
 */
public class Gatherer {

  private static final Logger log = LoggerFactory.getLogger(Gatherer.class);

  private ClientContext ctx;
  private String tableId;
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
    this.tableId = request.tableId;
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

    Iterable<TabletMetadata> tmi = MetadataScanner.builder().from(ctx).overTableId(tableId, startRow, endRow).fetchFiles().fetchLocation().fetchLast()
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
  }

  private class FilesProcessor implements Callable<ProcessedFiles> {

    HostAndPort location;
    Map<String,List<TRowRange>> allFiles;
    private TInfo tinfo;

    public FilesProcessor(TInfo tinfo, HostAndPort location, Map<String,List<TRowRange>> allFiles) {
      this.location = location;
      this.allFiles = allFiles;
      this.tinfo = tinfo;
    }

    @Override
    public ProcessedFiles call() throws Exception {
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

          TSummaries tSums = null;
          try {
            tSums = client.getSummariesFromFiles(tinfo, ctx.rpcCreds(), getRequest(), files);
          } catch (TApplicationException tae) {
            throw tae;
          } catch (TTransportException e) {
            pfiles.failedFiles.addAll(files.keySet());
            continue;
          }

          pfiles.summaries.merge(new SummaryCollection(tSums), factory);
        }
      } finally {
        ThriftUtil.returnClient(client);
      }

      return pfiles;
    }
  }

  /**
   * This methods reads a subset of file paths into memory and groups them by location. Then it request sumaries for files from each location/tablet server.
   */
  public SummaryCollection processPartition(int modulus, int remainder) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    Predicate<String> hashingFileSelector = file -> Math.abs(Hashing.murmur3_32().hashString(file).asInt()) % modulus == remainder;

    Map<String,Map<String,List<TRowRange>>> filesGBL = getFilesGroupedByLocation(hashingFileSelector);

    SummaryCollection partitionSummaries = new SummaryCollection();

    ExecutorService execSrv = Executors.newCachedThreadPool();
    try {
      TInfo tinfo = Tracer.traceInfo();
      while (true) {
        Set<String> failedFiles = new HashSet<>();

        List<Future<ProcessedFiles>> futures = new ArrayList<>();
        for (Entry<String,Map<String,List<TRowRange>>> entry : filesGBL.entrySet()) {
          HostAndPort location = HostAndPort.fromString(entry.getKey());
          Map<String,List<TRowRange>> allFiles = entry.getValue();

          Future<ProcessedFiles> future = execSrv.submit(new FilesProcessor(tinfo, location, allFiles));
          futures.add(future);
        }

        for (Future<ProcessedFiles> future : futures) {
          try {
            ProcessedFiles pfiles = future.get();
            if (pfiles.summaries.getTotalFiles() > 0) {
              partitionSummaries.merge(pfiles.summaries, factory);
            }

            failedFiles.addAll(pfiles.failedFiles);
          } catch (InterruptedException | ExecutionException e) {
            throw new AccumuloException(e);
          }
        }

        if (failedFiles.size() > 0) {
          // get new locations just for failed files
          Predicate<String> fileSelector = hashingFileSelector.and(file -> failedFiles.contains(file));
          filesGBL = getFilesGroupedByLocation(fileSelector);
          UtilWaitThread.sleep(250);
        } else {
          break;
        }
      }
    } finally {
      execSrv.shutdownNow();
    }

    return partitionSummaries;
  }

  public static interface FileSystemResolver {
    FileSystem get(Path file);
  }

  /**
   * This method will read summaries from a set of files.
   */
  public SummaryCollection processFiles(FileSystemResolver volMgr, Map<String,List<TRowRange>> files, BlockCache summaryCache, BlockCache indexCache,
      ExecutorService srp) {
    SummaryCollection fileSummaries = new SummaryCollection();

    List<Future<SummaryCollection>> futures = new ArrayList<>();
    for (Entry<String,List<TRowRange>> entry : files.entrySet()) {
      Future<SummaryCollection> future = srp.submit(() -> {
        List<RowRange> rrl = Lists.transform(entry.getValue(), RowRange::new);
        return getSummaries(volMgr, entry.getKey(), rrl, summaryCache, indexCache);
      });
      futures.add(future);
    }

    for (Future<SummaryCollection> future : futures) {
      try {
        fileSummaries.merge(future.get(), factory);
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    return fileSummaries;
  }

  private int countFiles() throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    // TODO use a batch scanner + iterator to parallelize counting files
    Iterable<TabletMetadata> tmi = MetadataScanner.builder().from(ctx).overTableId(tableId, startRow, endRow).fetchFiles().fetchPrev().build();
    return (int) StreamSupport.stream(tmi.spliterator(), false).mapToInt(tm -> tm.getFiles().size()).sum();
  }

  private class GatherRequest implements Callable<SummaryCollection> {

    private int remainder;
    private int modulus;
    private TInfo tinfo;

    GatherRequest(TInfo tinfo, int remainder, int modulus) {
      this.remainder = remainder;
      this.modulus = modulus;
      this.tinfo = tinfo;
    }

    @Override
    public SummaryCollection call() throws Exception {
      TSummaryRequest req = getRequest();

      ClientContext cct = new ClientContext(ctx.getInstance(), ctx.getCredentials(), ctx.getConfiguration()) {
        @Override
        public long getClientTimeoutInMillis() {
          return Math.max(super.getClientTimeoutInMillis(), 10 * 60 * 1000);
        }
      };

      TSummaries tSums = ServerClient.execute(cct, c -> c.getSummariesForPartition(tinfo, ctx.rpcCreds(), req, modulus, remainder));
      return new SummaryCollection(tSums);
    }
  }

  public SummaryCollection gather() throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    ExecutorService es = Executors.newFixedThreadPool(ctx.getConnector().instanceOperations().getTabletServers().size());
    try {

      int numFiles = countFiles();

      log.debug("Gathering summaries from {} files", numFiles);

      // have each tablet server process ~100K files
      int numRequest = Math.max(numFiles / 100_000, 1);

      List<Future<SummaryCollection>> futures = new ArrayList<>();

      TInfo tinfo = Tracer.traceInfo();
      for (int i = 0; i < numRequest; i++) {
        futures.add(es.submit(new GatherRequest(tinfo, i, numRequest)));
      }

      SummaryCollection allSummaries = new SummaryCollection();
      for (Future<SummaryCollection> future : futures) {
        try {
          allSummaries.merge(future.get(), factory);
        } catch (InterruptedException | ExecutionException e) {
          throw new AccumuloException(e);
        }
      }

      return allSummaries;
    } finally {
      es.shutdownNow();
    }
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

    try {
      Path path = new Path(file);
      Configuration conf = CachedConfiguration.getInstance();
      return SummaryReader.load(volMgr.get(path), conf, ctx.getConfiguration(), factory, path, summarySelector, summaryCache, indexCache).getSummaries(ranges);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
