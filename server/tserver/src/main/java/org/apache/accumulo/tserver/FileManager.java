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
package org.apache.accumulo.tserver;

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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.InterruptibleIterator;
import org.apache.accumulo.core.iterators.system.SourceSwitchingIterator;
import org.apache.accumulo.core.iterators.system.SourceSwitchingIterator.DataSource;
import org.apache.accumulo.core.iterators.system.TimeSettingIterator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReportingIterator;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class FileManager {

  private static final Logger log = Logger.getLogger(FileManager.class);

  int maxOpen;

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
      if (releaseTime < o.releaseTime) {
        return -1;
      } else if (releaseTime > o.releaseTime) {
        return 1;
      } else {
        return 0;
      }
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

  private VolumeManager fs;

  // the data cache and index cache are allocated in
  // TabletResourceManager and passed through the file opener to
  // CachableBlockFile which can handle the caches being
  // null if unallocated
  private BlockCache dataCache = null;
  private BlockCache indexCache = null;

  private long maxIdleTime;

  private final ServerConfiguration conf;

  private class IdleFileCloser implements Runnable {

    @Override
    public void run() {

      long curTime = System.currentTimeMillis();

      ArrayList<FileSKVIterator> filesToClose = new ArrayList<FileSKVIterator>();

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

          if (ofl.size() == 0) {
            iter.remove();
          }
        }
      }

      closeReaders(filesToClose);

    }

  }

  /**
   *
   * @param dataCache
   *          : underlying file can and should be able to handle a null cache
   * @param indexCache
   *          : underlying file can and should be able to handle a null cache
   */
  FileManager(ServerConfiguration conf, VolumeManager fs, int maxOpen, BlockCache dataCache, BlockCache indexCache) {

    if (maxOpen <= 0)
      throw new IllegalArgumentException("maxOpen <= 0");
    this.conf = conf;
    this.dataCache = dataCache;
    this.indexCache = indexCache;

    this.filePermits = new Semaphore(maxOpen, true);
    this.maxOpen = maxOpen;
    this.fs = fs;

    this.openFiles = new HashMap<String,List<OpenReader>>();
    this.reservedReaders = new HashMap<FileSKVIterator,String>();

    this.maxIdleTime = conf.getConfiguration().getTimeInMillis(Property.TSERV_MAX_IDLE);
    SimpleTimer.getInstance().schedule(new IdleFileCloser(), maxIdleTime, maxIdleTime / 2);

  }

  private static int countReaders(Map<String,List<OpenReader>> files) {
    int count = 0;

    for (List<OpenReader> list : files.values()) {
      count += list.size();
    }

    return count;
  }

  private List<FileSKVIterator> takeLRUOpenFiles(int numToTake) {

    ArrayList<OpenReader> openReaders = new ArrayList<OpenReader>();

    for (Entry<String,List<OpenReader>> entry : openFiles.entrySet()) {
      openReaders.addAll(entry.getValue());
    }

    Collections.sort(openReaders);

    ArrayList<FileSKVIterator> ret = new ArrayList<FileSKVIterator>();

    for (int i = 0; i < numToTake && i < openReaders.size(); i++) {
      OpenReader or = openReaders.get(i);

      List<OpenReader> ofl = openFiles.get(or.fileName);
      if (!ofl.remove(or)) {
        throw new RuntimeException("Failed to remove open reader that should have been there");
      }

      if (ofl.size() == 0) {
        openFiles.remove(or.fileName);
      }

      ret.add(or.reader);
    }

    return ret;
  }

  private static <T> List<T> getFileList(String file, Map<String,List<T>> files) {
    List<T> ofl = files.get(file);
    if (ofl == null) {
      ofl = new ArrayList<T>();
      files.put(file, ofl);
    }

    return ofl;
  }

  private void closeReaders(List<FileSKVIterator> filesToClose) {
    for (FileSKVIterator reader : filesToClose) {
      try {
        reader.close();
      } catch (Exception e) {
        log.error("Failed to close file " + e.getMessage(), e);
      }
    }
  }

  private List<String> takeOpenFiles(Collection<String> files, List<FileSKVIterator> reservedFiles, Map<FileSKVIterator,String> readersReserved) {
    List<String> filesToOpen = new LinkedList<String>(files);
    for (Iterator<String> iterator = filesToOpen.iterator(); iterator.hasNext();) {
      String file = iterator.next();

      List<OpenReader> ofl = openFiles.get(file);
      if (ofl != null && ofl.size() > 0) {
        OpenReader openReader = ofl.remove(ofl.size() - 1);
        reservedFiles.add(openReader.reader);
        readersReserved.put(openReader.reader, file);
        if (ofl.size() == 0) {
          openFiles.remove(file);
        }
        iterator.remove();
      }

    }
    return filesToOpen;
  }

  private synchronized String getReservedReadeFilename(FileSKVIterator reader) {
    return reservedReaders.get(reader);
  }

  private List<FileSKVIterator> reserveReaders(KeyExtent tablet, Collection<String> files, boolean continueOnFailure) throws IOException {

    if (!tablet.isMeta() && files.size() >= maxOpen) {
      throw new IllegalArgumentException("requested files exceeds max open");
    }

    if (files.size() == 0) {
      return Collections.emptyList();
    }

    List<String> filesToOpen = null;
    List<FileSKVIterator> filesToClose = Collections.emptyList();
    List<FileSKVIterator> reservedFiles = new ArrayList<FileSKVIterator>();
    Map<FileSKVIterator,String> readersReserved = new HashMap<FileSKVIterator,String>();

    if (!tablet.isMeta()) {
      filePermits.acquireUninterruptibly(files.size());
    }

    // now that the we are past the semaphore, we have the authority
    // to open files.size() files

    // determine what work needs to be done in sync block
    // but do the work of opening and closing files outside
    // a synch block
    synchronized (this) {

      filesToOpen = takeOpenFiles(files, reservedFiles, readersReserved);

      int numOpen = countReaders(openFiles);

      if (filesToOpen.size() + numOpen + reservedReaders.size() > maxOpen) {
        filesToClose = takeLRUOpenFiles((filesToOpen.size() + numOpen + reservedReaders.size()) - maxOpen);
      }
    }

    // close files before opening files to ensure we stay under resource
    // limitations
    closeReaders(filesToClose);

    // open any files that need to be opened
    for (String file : filesToOpen) {
      try {
        if (!file.contains(":"))
          throw new IllegalArgumentException("Expected uri, got : " + file);
        Path path = new Path(file);
        FileSystem ns = fs.getVolumeByPath(path).getFileSystem();
        // log.debug("Opening "+file + " path " + path);
        FileSKVIterator reader = FileOperations.getInstance().openReader(path.toString(), false, ns, ns.getConf(), conf.getTableConfiguration(tablet),
            dataCache, indexCache);
        reservedFiles.add(reader);
        readersReserved.put(reader, file);
      } catch (Exception e) {

        ProblemReports.getInstance().report(new ProblemReport(tablet.toString(), ProblemType.FILE_READ, file, e));

        if (continueOnFailure) {
          // release the permit for the file that failed to open
          if (!tablet.isMeta()) {
            filePermits.release(1);
          }
          log.warn("Failed to open file " + file + " " + e.getMessage() + " continuing...");
        } else {
          // close whatever files were opened
          closeReaders(reservedFiles);

          if (!tablet.isMeta()) {
            filePermits.release(files.size());
          }

          log.error("Failed to open file " + file + " " + e.getMessage());
          throw new IOException("Failed to open " + file, e);
        }
      }
    }

    synchronized (this) {
      // update set of reserved readers
      reservedReaders.putAll(readersReserved);
    }

    return reservedFiles;
  }

  private void releaseReaders(KeyExtent tablet, List<FileSKVIterator> readers, boolean sawIOException) {
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
          log.warn(e, e);
          sawIOException = true;
        }
      }

      for (FileSKVIterator reader : readers) {
        String fileName = reservedReaders.remove(reader);
        if (!sawIOException)
          getFileList(fileName, openFiles).add(new OpenReader(fileName, reader));
      }
    }

    if (sawIOException)
      closeReaders(readers);

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
      this.deepCopies = new ArrayList<FileManager.FileDataSource>();
    }

    public FileDataSource(IteratorEnvironment env, SortedKeyValueIterator<Key,Value> deepCopy, ArrayList<FileDataSource> deepCopies) {
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
    public SortedKeyValueIterator<Key,Value> iterator() throws IOException {
      if (iflag != null)
        ((InterruptibleIterator) this.iter).setInterruptFlag(iflag);
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

      if (iflag != null)
        ((InterruptibleIterator) this.iter).setInterruptFlag(iflag);

      for (FileDataSource fds : deepCopies) {
        fds.current = false;
        fds.iter = iter.deepCopy(fds.env);
      }
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      this.iflag = flag;
    }

  }

  public class ScanFileManager {

    private ArrayList<FileDataSource> dataSources;
    private ArrayList<FileSKVIterator> tabletReservedReaders;
    private KeyExtent tablet;
    private boolean continueOnFailure;

    ScanFileManager(KeyExtent tablet) {
      tabletReservedReaders = new ArrayList<FileSKVIterator>();
      dataSources = new ArrayList<FileDataSource>();
      this.tablet = tablet;

      continueOnFailure = conf.getTableConfiguration(tablet).getBoolean(Property.TABLE_FAILURES_IGNORE);

      if (tablet.isMeta()) {
        continueOnFailure = false;
      }
    }

    private List<FileSKVIterator> openFileRefs(Collection<FileRef> files) throws TooManyFilesException, IOException {
      List<String> strings = new ArrayList<String>(files.size());
      for (FileRef ref : files)
        strings.add(ref.path().toString());
      return openFiles(strings);
    }

    private List<FileSKVIterator> openFiles(Collection<String> files) throws TooManyFilesException, IOException {
      // one tablet can not open more than maxOpen files, otherwise it could get stuck
      // forever waiting on itself to release files

      if (tabletReservedReaders.size() + files.size() >= maxOpen) {
        throw new TooManyFilesException("Request to open files would exceed max open files reservedReaders.size()=" + tabletReservedReaders.size()
            + " files.size()=" + files.size() + " maxOpen=" + maxOpen + " tablet = " + tablet);
      }

      List<FileSKVIterator> newlyReservedReaders = reserveReaders(tablet, files, continueOnFailure);

      tabletReservedReaders.addAll(newlyReservedReaders);
      return newlyReservedReaders;
    }

    synchronized List<InterruptibleIterator> openFiles(Map<FileRef,DataFileValue> files, boolean detachable) throws IOException {

      List<FileSKVIterator> newlyReservedReaders = openFileRefs(files.keySet());

      ArrayList<InterruptibleIterator> iters = new ArrayList<InterruptibleIterator>();

      for (FileSKVIterator reader : newlyReservedReaders) {
        String filename = getReservedReadeFilename(reader);
        InterruptibleIterator iter;
        if (detachable) {
          FileDataSource fds = new FileDataSource(filename, reader);
          dataSources.add(fds);
          SourceSwitchingIterator ssi = new SourceSwitchingIterator(fds);
          iter = new ProblemReportingIterator(tablet.getTableId().toString(), filename, continueOnFailure, ssi);
        } else {
          iter = new ProblemReportingIterator(tablet.getTableId().toString(), filename, continueOnFailure, reader);
        }
        DataFileValue value = files.get(new FileRef(filename));
        if (value.isTimeSet()) {
          iter = new TimeSettingIterator(iter, value.getTime());
        }

        iters.add(iter);
      }

      return iters;
    }

    synchronized void detach() {

      releaseReaders(tablet, tabletReservedReaders, false);
      tabletReservedReaders.clear();

      for (FileDataSource fds : dataSources)
        fds.unsetIterator();
    }

    synchronized void reattach() throws IOException {
      if (tabletReservedReaders.size() != 0)
        throw new IllegalStateException();

      Collection<String> files = new ArrayList<String>();
      for (FileDataSource fds : dataSources)
        files.add(fds.file);

      List<FileSKVIterator> newlyReservedReaders = openFiles(files);
      Map<String,List<FileSKVIterator>> map = new HashMap<String,List<FileSKVIterator>>();
      for (FileSKVIterator reader : newlyReservedReaders) {
        String fileName = getReservedReadeFilename(reader);
        List<FileSKVIterator> list = map.get(fileName);
        if (list == null) {
          list = new LinkedList<FileSKVIterator>();
          map.put(fileName, list);
        }

        list.add(reader);
      }

      for (FileDataSource fds : dataSources) {
        FileSKVIterator reader = map.get(fds.file).remove(0);
        fds.setIterator(reader);
      }
    }

    synchronized void releaseOpenFiles(boolean sawIOException) {
      releaseReaders(tablet, tabletReservedReaders, sawIOException);
      tabletReservedReaders.clear();
      dataSources.clear();
    }

    synchronized int getNumOpenFiles() {
      return tabletReservedReaders.size();
    }
  }

  public ScanFileManager newScanFileManager(KeyExtent tablet) {
    return new ScanFileManager(tablet);
  }
}
