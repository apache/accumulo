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
package org.apache.accumulo.tserver.log;

import static org.apache.accumulo.core.client.Accumulo.EMPTY_BYTES;
import static org.apache.accumulo.core.client.Accumulo.EMPTY_RANGE;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.conf.IterConfigUtil;
import org.apache.accumulo.core.conf.IterLoad;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.iterators.IteratorAdapter;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

/**
 * Iterates over multiple sorted recovery logs merging them into a single sorted stream.
 */
public class RecoveryLogsIterator
    implements Iterator<Entry<LogFileKey,LogFileValue>>, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(RecoveryLogsIterator.class);

  private final List<FileSKVIterator> scanners;
  private final Iterator<Entry<Key,Value>> iter;
  private final CryptoService cryptoService;

  /**
   * Scans the files in each recoveryLogDir over the range [start,end].
   */
  public RecoveryLogsIterator(ServerContext context, List<Path> recoveryLogDirs, LogFileKey start,
      LogFileKey end, boolean checkFirstKey) throws IOException {
    cryptoService = context.getCryptoService();

    List<Iterator<Entry<Key,Value>>> iterators = new ArrayList<>(recoveryLogDirs.size());
    scanners = new ArrayList<>();
    Collection<ByteSequence> columnFamiles = new ArrayList<>();
    Range range;
    if (start == null) {
      range = null;
    } else {
      range = LogFileKey.toRange(start, end);
      columnFamiles.add(start.getColumnFamily());
      columnFamiles.add(end.getColumnFamily());
    }

    var vm = context.getVolumeManager();
    var recoveryCacheMap = context.getRecoveryCacheMap();

    for (Path logDir : recoveryLogDirs) {
      var recoveryCache = recoveryCacheMap.get(logDir);
      LOG.debug("Opening recovery log dir {}", logDir.getName());
      List<Path> logFiles = recoveryCache.getLogFiles();
      CacheProvider cacheProvider = recoveryCache.getCacheProvider();
      var fs = vm.getFileSystemByPath(logDir);

      // only check the first key once to prevent extra iterator creation and seeking
      if (checkFirstKey) {
        validateFirstKey(context, fs, logFiles, logDir);
      }
      //ListIterator<FileSKVIterator> iteratorList = recoveryCache.getScanners().listIterator();
      for (Path log : logFiles) {
        //FileSKVIterator iterator = iteratorList.next();
        //iterator.seek(range, columnFamiles, true);
        Iterator<Entry<Key, Value>> iterator = createIterator(context, log, cacheProvider);

        if (iterator.hasTop()) {
          LOG.debug("Write ahead log {} has data in range {} {}", log.getName(), start, end);
          iterators.add(toIterator(context, iterator, cacheProvider));
          scanners.addAll(recoveryCache.getScanners());
        } else {
          LOG.debug("Write ahead log {} has no data in range {} {}", log.getName(), start, end);
        }
      }
    }
    iter = Iterators.mergeSorted(iterators, Entry.comparingByKey());
  }

  private Iterator<Entry<Key, Value>> createIterator(ServerContext context, Path wal,
                                                     CacheProvider cacheProvider) throws IOException {
    var fs = context.getVolumeManager().getFileSystemByPath(wal);
    // ScannerImpl recoveryFileScanner = new ScannerImpl(context, tableId, new Authorizations());
    FileSKVIterator fileReader =
            FileOperations.getInstance().newReaderBuilder().withCacheProvider(cacheProvider)
                    .forFile(wal.getName(), fs, context.getHadoopConf(), context.getCryptoService())
                    .withTableConfiguration(context.getConfiguration()).seekToBeginning().build();


  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Entry<LogFileKey,LogFileValue> next() {
    Entry<Key,Value> e = iter.next();
    return new AbstractMap.SimpleImmutableEntry<>(LogFileKey.fromKey(e.getKey()),
        LogFileValue.fromValue(e.getValue()));
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public void close() throws IOException {
    for (FileSKVIterator scanner : scanners) {
      scanner.close();
    }
  }

  /**
   * Check that the first entry in the WAL is OPEN. Only need to do this once.
   */
  private void validateFirstKey(ServerContext context, FileSystem fs, List<Path> logFiles,
      Path fullLogPath) {
    try (var scanner =
        RFile.newScanner().from(logFiles.stream().map(Path::toString).toArray(String[]::new))
            .withFileSystem(fs).withTableProperties(context.getConfiguration()).build()) {
      Iterator<Entry<Key,Value>> iterator = scanner.iterator();
      if (iterator.hasNext()) {
        Key firstKey = iterator.next().getKey();
        LogFileKey key = LogFileKey.fromKey(firstKey);
        if (key.event != LogEvents.OPEN) {
          throw new IllegalStateException("First log entry is not OPEN " + fullLogPath);
        }
      }
    }
  }

  protected List<IterInfo> serverSideIteratorList = Collections.emptyList();
  protected Map<String,Map<String,String>> serverSideIteratorOptions = Collections.emptyMap();

  private Iterator<Entry<Key,Value>> toIterator(ServerContext context, FileSKVIterator fileIterator,
      CacheProvider cacheProvider) {
    boolean useSystemIterators = false;
    Range bounds = new Range();
    //long fileLength = 24234134L; // TODO
    String filename = "";

    try {
      List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<>();
      //FSDataInputStream inputStream = (FSDataInputStream) fileIterator;
      //CachableBlockFile.CachableBuilder cb = new CachableBlockFile.CachableBuilder()
       //   .input(inputStream, "source-1").length(fileLength).conf(context.getHadoopConf())
        //  .cacheProvider(cacheProvider).cryptoService(cryptoService);
      //readers.add(new org.apache.accumulo.core.file.rfile.RFile.Reader(cb));

      SortedKeyValueIterator<Key,Value> iterator;
      if (bounds != null) {
        iterator = new MultiIterator(readers, bounds);
      } else {
        iterator = new MultiIterator(readers, false);
      }

      Set<ByteSequence> families = Collections.emptySet();

      if (useSystemIterators) {
        SortedSet<Column> cols = null;
        families = LocalityGroupUtil.families(cols);
        iterator = SystemIteratorUtil.setupSystemScanIterators(iterator, cols, Authorizations.EMPTY,
            EMPTY_BYTES, context.getConfiguration());
      }

      try {
        iterator =
            IterConfigUtil.loadIterators(iterator, new IterLoad().iters(serverSideIteratorList)
                .iterOpts(serverSideIteratorOptions).iterEnv(null).useAccumuloClassLoader(false));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      iterator.seek(bounds == null ? EMPTY_RANGE : bounds, families, !families.isEmpty());
      return new IteratorAdapter(iterator);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
