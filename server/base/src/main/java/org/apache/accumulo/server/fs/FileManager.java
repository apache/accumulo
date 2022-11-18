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
package org.apache.accumulo.server.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SourceSwitchingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SourceSwitchingIterator.DataSource;
import org.apache.accumulo.core.iteratorsImpl.system.TimeSettingIterator;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReportingIterator;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

public class FileManager {

  private static final Logger log = LoggerFactory.getLogger(FileManager.class);

  private int maxOpen;

  private static class OpenReader implements Comparable<OpenReader> {
    long releaseTime;
    FileSKVIterator reader;
    String fileName;

    public OpenReader(String fileName, FileSKVIterator reader) {
      this.fileName = fileName;
      this.reader = reader;
      this.releaseTime = System.currentTimeMillis();
    }

    @Override
    public int compareTo(OpenReader o) {
      return Long.compare(releaseTime, o.releaseTime);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof OpenReader) {
        return compareTo((OpenReader) obj) == 0;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return fileName.hashCode();
    }
  }

  private Map<String,List<OpenReader>> openFiles;
  private HashMap<FileSKVIterator,String> reservedReaders;

  private Semaphore filePermits;

  private Cache<String,Long> fileLenCache;

  private long maxIdleTime;
  private long slowFilePermitMillis;

  private final ServerContext context;

  private class IdleFileCloser implements Runnable {

    @Override
    public void run() {

      long curTime = System.currentTimeMillis();

      ArrayList<FileSKVIterator> filesToClose = new ArrayList<>();

      // determine which files to close in a sync block, and then close the
      // files outside of the sync block
      synchronized (FileManager.this) {
        Iterator<Entry<String,List<OpenReader>>> iter = openFiles.entrySet().iterator();
        while (iter.hasNext()) {
          Entry<String,List<OpenReader>> entry = iter.next();
          List<OpenReader> ofl = entry.getValue();

          for (Iterator<OpenReader> oflIter = ofl.iterator(); oflIter.hasNext();) {
            OpenReader openReader = oflIter.next();

            if (curTime - openReader.releaseTime > maxIdleTime) {

              filesToClose.add(openReader.reader);
              oflIter.remove();
            }
          }

          if (ofl.isEmpty()) {
            iter.remove();
          }
        }
      }

      closeReaders(filesToClose);

    }

  }

  public FileManager(ServerContext context, int maxOpen, Cache<String,Long> fileLenCache) {

    if (maxOpen <= 0) {
      throw new IllegalArgumentException("maxOpen <= 0");
    }
    this.context = context;
    this.fileLenCache = fileLenCache;

    this.filePermits = new Semaphore(maxOpen, false);
    this.maxOpen = maxOpen;

    this.openFiles = new HashMap<>();
    this.reservedReaders = new HashMap<>();

    this.maxIdleTime = this.context.getConfiguration().getTimeInMillis(Property.TSERV_MAX_IDLE);
    ThreadPools.watchCriticalScheduledTask(
        this.context.getScheduledExecutor().scheduleWithFixedDelay(new IdleFileCloser(),
            maxIdleTime, maxIdleTime / 2, TimeUnit.MILLISECONDS));

    this.slowFilePermitMillis =
        this.context.getConfiguration().getTimeInMillis(Property.TSERV_SLOW_FILEPERMIT_MILLIS);
  }

  private static int countReaders(Map<String,List<OpenReader>> files) {
    int count = 0;

    for (List<OpenReader> list : files.values()) {
      count += list.size();
    }

    return count;
  }

  private List<FileSKVIterator> takeLRUOpenFiles(int numToTake) {

    ArrayList<OpenReader> openReaders = new ArrayList<>();

    for (Entry<String,List<OpenReader>> entry : openFiles.entrySet()) {
      openReaders.addAll(entry.getValue());
    }

    Collections.sort(openReaders);

    ArrayList<FileSKVIterator> ret = new ArrayList<>();

    for (int i = 0; i < numToTake && i < openReaders.size(); i++) {
      OpenReader or = openReaders.get(i);

      List<OpenReader> ofl = openFiles.get(or.fileName);
      if (!ofl.remove(or)) {
        throw new RuntimeException("Failed to remove open reader that should have been there");
      }

      if (ofl.isEmpty()) {
        openFiles.remove(or.fileName);
      }

      ret.add(or.reader);
    }

    return ret;
  }

  private void closeReaders(Collection<FileSKVIterator> filesToClose) {
    for (FileSKVIterator reader : filesToClose) {
      try {
        reader.close();
      } catch (Exception e) {
        log.error("Failed to close file {}", e.getMessage(), e);
      }
    }
  }

  private List<String> takeOpenFiles(Collection<String> files,
      Map<FileSKVIterator,String> readersReserved) {
    List<String> filesToOpen = Collections.emptyList();
    for (String file : files) {
      List<OpenReader> ofl = openFiles.get(file);
      if (ofl != null && !ofl.isEmpty()) {
        OpenReader openReader = ofl.remove(ofl.size() - 1);
        readersReserved.put(openReader.reader, file);
        if (ofl.isEmpty()) {
          openFiles.remove(file);
        }
      } else {
        if (filesToOpen.isEmpty()) {
          filesToOpen = new ArrayList<>(files.size());
        }
        filesToOpen.add(file);
      }
    }
    return filesToOpen;
  }

  private Map<FileSKVIterator,String> reserveReaders(KeyExtent tablet, Collection<String> files,
      boolean continueOnFailure, CacheProvider cacheProvider) throws IOException {

    if (!tablet.isMeta() && files.size() >= maxOpen) {
      throw new IllegalArgumentException("requested files exceeds max open");
    }

    if (files.isEmpty()) {
      return Collections.emptyMap();
    }

    List<String> filesToOpen = null;
    List<FileSKVIterator> filesToClose = Collections.emptyList();
    Map<FileSKVIterator,String> readersReserved = new HashMap<>();

    if (!tablet.isMeta()) {
      long start = System.currentTimeMillis();
      filePermits.acquireUninterruptibly(files.size());
      long waitTime = System.currentTimeMillis() - start;

      if (waitTime >= slowFilePermitMillis) {
        log.info("Slow file permits request: {} ms, files requested: {}, tablet: {}", waitTime,
            files.size(), tablet);
      }
    }

    // now that the we are past the semaphore, we have the authority
    // to open files.size() files

    // determine what work needs to be done in sync block
    // but do the work of opening and closing files outside
    // a synch block
    synchronized (this) {

      filesToOpen = takeOpenFiles(files, readersReserved);

      if (!filesToOpen.isEmpty()) {
        int numOpen = countReaders(openFiles);

        if (filesToOpen.size() + numOpen + reservedReaders.size() > maxOpen) {
          filesToClose =
              takeLRUOpenFiles((filesToOpen.size() + numOpen + reservedReaders.size()) - maxOpen);
        }
      }
    }

    readersReserved.forEach((k, v) -> k.setCacheProvider(cacheProvider));

    // close files before opening files to ensure we stay under resource
    // limitations
    closeReaders(filesToClose);

    // open any files that need to be opened
    for (String file : filesToOpen) {
      try {
        if (!file.contains(":")) {
          throw new IllegalArgumentException("Expected uri, got : " + file);
        }
        Path path = new Path(file);
        FileSystem ns = context.getVolumeManager().getFileSystemByPath(path);
        // log.debug("Opening "+file + " path " + path);
        var tableConf = context.getTableConfiguration(tablet.tableId());
        FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder()
            .forFile(path.toString(), ns, ns.getConf(), tableConf.getCryptoService())
            .withTableConfiguration(tableConf).withCacheProvider(cacheProvider)
            .withFileLenCache(fileLenCache).build();
        readersReserved.put(reader, file);
      } catch (Exception e) {

        ProblemReports.getInstance(context)
            .report(new ProblemReport(tablet.tableId(), ProblemType.FILE_READ, file, e));

        if (continueOnFailure) {
          // release the permit for the file that failed to open
          if (!tablet.isMeta()) {
            filePermits.release(1);
          }
          log.warn("Failed to open file {} {} continuing...", file, e.getMessage(), e);
        } else {
          // close whatever files were opened
          closeReaders(readersReserved.keySet());

          if (!tablet.isMeta()) {
            filePermits.release(files.size());
          }

          log.error("Failed to open file {} {}", file, e.getMessage());
          throw new IOException("Failed to open " + file, e);
        }
      }
    }

    synchronized (this) {
      // update set of reserved readers
      reservedReaders.putAll(readersReserved);
    }

    return readersReserved;
  }

  private void releaseReaders(KeyExtent tablet, List<FileSKVIterator> readers,
      boolean sawIOException) {
    // put files in openFiles

    synchronized (this) {

      // check that readers were actually reserved ... want to make sure a thread does
      // not try to release readers they never reserved
      if (!reservedReaders.keySet().containsAll(readers)) {
        throw new IllegalArgumentException("Asked to release readers that were never reserved ");
      }

      for (FileSKVIterator reader : readers) {
        try {
          reader.closeDeepCopies();
        } catch (IOException e) {
          log.warn("{}", e.getMessage(), e);
          sawIOException = true;
        }
      }

      for (FileSKVIterator reader : readers) {
        String fileName = reservedReaders.remove(reader);
        if (!sawIOException) {
          openFiles.computeIfAbsent(fileName, k -> new ArrayList<>())
              .add(new OpenReader(fileName, reader));
        }
      }
    }

    if (sawIOException) {
      closeReaders(readers);
    }

    // decrement the semaphore
    if (!tablet.isMeta()) {
      filePermits.release(readers.size());
    }

  }

  static class FileDataSource implements DataSource {

    private SortedKeyValueIterator<Key,Value> iter;
    private ArrayList<FileDataSource> deepCopies;
    private boolean current = true;
    private IteratorEnvironment env;
    private String file;
    private AtomicBoolean iflag;

    FileDataSource(String file, SortedKeyValueIterator<Key,Value> iter) {
      this.file = file;
      this.iter = iter;
      this.deepCopies = new ArrayList<>();
    }

    public FileDataSource(IteratorEnvironment env, SortedKeyValueIterator<Key,Value> deepCopy,
        ArrayList<FileDataSource> deepCopies) {
      this.iter = deepCopy;
      this.env = env;
      this.deepCopies = deepCopies;
      deepCopies.add(this);
    }

    @Override
    public boolean isCurrent() {
      return current;
    }

    @Override
    public DataSource getNewDataSource() {
      current = true;
      return this;
    }

    @Override
    public DataSource getDeepCopyDataSource(IteratorEnvironment env) {
      return new FileDataSource(env, iter.deepCopy(env), deepCopies);
    }

    @Override
    public SortedKeyValueIterator<Key,Value> iterator() {
      return iter;
    }

    void unsetIterator() {
      current = false;
      iter = null;
      for (FileDataSource fds : deepCopies) {
        fds.current = false;
        fds.iter = null;
      }
    }

    void setIterator(SortedKeyValueIterator<Key,Value> iter) {
      current = false;
      this.iter = iter;

      if (iflag != null) {
        ((InterruptibleIterator) this.iter).setInterruptFlag(iflag);
      }

      for (FileDataSource fds : deepCopies) {
        fds.current = false;
        fds.iter = iter.deepCopy(fds.env);
      }
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      this.iflag = flag;
      ((InterruptibleIterator) this.iter).setInterruptFlag(iflag);
    }
  }

  public class ScanFileManager {

    private ArrayList<FileDataSource> dataSources;
    private ArrayList<FileSKVIterator> tabletReservedReaders;
    private KeyExtent tablet;
    private boolean continueOnFailure;
    private CacheProvider cacheProvider;

    ScanFileManager(KeyExtent tablet, CacheProvider cacheProvider) {
      tabletReservedReaders = new ArrayList<>();
      dataSources = new ArrayList<>();
      this.tablet = tablet;
      this.cacheProvider = cacheProvider;

      continueOnFailure = context.getTableConfiguration(tablet.tableId())
          .getBoolean(Property.TABLE_FAILURES_IGNORE);

      if (tablet.isMeta()) {
        continueOnFailure = false;
      }
    }

    private Map<FileSKVIterator,String> openFiles(List<String> files)
        throws TooManyFilesException, IOException {
      // one tablet can not open more than maxOpen files, otherwise it could get stuck
      // forever waiting on itself to release files

      if (tabletReservedReaders.size() + files.size() >= maxOpen) {
        throw new TooManyFilesException(
            "Request to open files would exceed max open files reservedReaders.size()="
                + tabletReservedReaders.size() + " files.size()=" + files.size() + " maxOpen="
                + maxOpen + " tablet = " + tablet);
      }

      Map<FileSKVIterator,String> newlyReservedReaders =
          reserveReaders(tablet, files, continueOnFailure, cacheProvider);

      tabletReservedReaders.addAll(newlyReservedReaders.keySet());
      return newlyReservedReaders;
    }

    public synchronized List<InterruptibleIterator> openFiles(Map<TabletFile,DataFileValue> files,
        boolean detachable, SamplerConfigurationImpl samplerConfig) throws IOException {

      Map<FileSKVIterator,String> newlyReservedReaders = openFiles(
          files.keySet().stream().map(TabletFile::getPathStr).collect(Collectors.toList()));

      ArrayList<InterruptibleIterator> iters = new ArrayList<>();

      boolean sawTimeSet = files.values().stream().anyMatch(DataFileValue::isTimeSet);

      for (Entry<FileSKVIterator,String> entry : newlyReservedReaders.entrySet()) {
        FileSKVIterator source = entry.getKey();
        String filename = entry.getValue();
        InterruptibleIterator iter;

        if (samplerConfig != null) {
          source = source.getSample(samplerConfig);
          if (source == null) {
            throw new SampleNotPresentException();
          }
        }

        iter = new ProblemReportingIterator(context, tablet.tableId(), filename, continueOnFailure,
            detachable ? getSsi(filename, source) : source);

        if (sawTimeSet) {
          // constructing FileRef is expensive so avoid if not needed
          DataFileValue value = files.get(new TabletFile(new Path(filename)));
          if (value.isTimeSet()) {
            iter = new TimeSettingIterator(iter, value.getTime());
          }
        }

        iters.add(iter);
      }

      return iters;
    }

    private SourceSwitchingIterator getSsi(String filename, FileSKVIterator source) {
      FileDataSource fds = new FileDataSource(filename, source);
      dataSources.add(fds);
      return new SourceSwitchingIterator(fds);
    }

    public synchronized void detach() {

      releaseReaders(tablet, tabletReservedReaders, false);
      tabletReservedReaders.clear();

      for (FileDataSource fds : dataSources) {
        fds.unsetIterator();
      }
    }

    public synchronized void reattach(SamplerConfigurationImpl samplerConfig) throws IOException {
      if (!tabletReservedReaders.isEmpty()) {
        throw new IllegalStateException();
      }

      List<String> files = dataSources.stream().map(x -> x.file).collect(Collectors.toList());
      Map<FileSKVIterator,String> newlyReservedReaders = openFiles(files);
      Map<String,List<FileSKVIterator>> map = new HashMap<>();
      newlyReservedReaders.forEach(
          (reader, fileName) -> map.computeIfAbsent(fileName, k -> new LinkedList<>()).add(reader));

      for (FileDataSource fds : dataSources) {
        FileSKVIterator source = map.get(fds.file).remove(0);
        if (samplerConfig != null) {
          source = source.getSample(samplerConfig);
          if (source == null) {
            throw new SampleNotPresentException();
          }
        }
        fds.setIterator(source);
      }
    }

    public synchronized void releaseOpenFiles(boolean sawIOException) {
      releaseReaders(tablet, tabletReservedReaders, sawIOException);
      tabletReservedReaders.clear();
      dataSources.clear();
    }

    public synchronized int getNumOpenFiles() {
      return tabletReservedReaders.size();
    }
  }

  public ScanFileManager newScanFileManager(KeyExtent tablet, CacheProvider cacheProvider) {
    return new ScanFileManager(tablet, cacheProvider);
  }
}
