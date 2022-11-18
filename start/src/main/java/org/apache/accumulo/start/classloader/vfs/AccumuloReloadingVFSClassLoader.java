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
package org.apache.accumulo.start.classloader.vfs;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Classloader that delegates operations to a VFSClassLoader object. This class also listens for
 * changes in any of the files/directories that are in the classpath and will recreate the delegate
 * object if there is any change in the classpath.
 */
@Deprecated
public class AccumuloReloadingVFSClassLoader implements FileListener, ReloadingClassLoader {

  private static final Logger log = LoggerFactory.getLogger(AccumuloReloadingVFSClassLoader.class);

  // set to 5 mins. The rationale behind this large time is to avoid a gazillion tservers all asking
  // the name node for info too frequently.
  private static final long DEFAULT_TIMEOUT = MINUTES.toMillis(5);

  private volatile long maxWaitInterval = 60000;

  private volatile long maxRetries = -1;

  private volatile long sleepInterval = 5000;

  private FileObject[] files;
  private VFSClassLoader cl;
  private final ReloadingClassLoader parent;
  private final String uris;
  private final DefaultFileMonitor monitor;
  private final boolean preDelegate;
  private final ThreadPoolExecutor executor;
  {
    BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(2);
    ThreadFactory factory = r -> {
      Thread t = new Thread(r);
      t.setDaemon(true);
      return t;
    };
    executor = new ThreadPoolExecutor(1, 1, 1, SECONDS, queue, factory);
  }

  private final Runnable refresher = new Runnable() {
    @Override
    public void run() {
      while (!executor.isTerminating()) {
        try {
          FileSystemManager vfs = AccumuloVFSClassLoader.generateVfs();
          FileObject[] files = AccumuloVFSClassLoader.resolve(vfs, uris);

          long retries = 0;
          long currentSleepMillis = sleepInterval;

          if (files.length == 0) {
            while (files.length == 0 && retryPermitted(retries)) {

              try {
                log.debug("VFS path was empty.  Waiting " + currentSleepMillis + " ms to retry");
                Thread.sleep(currentSleepMillis);

                files = AccumuloVFSClassLoader.resolve(vfs, uris);
                retries++;

                currentSleepMillis = Math.min(maxWaitInterval, currentSleepMillis + sleepInterval);

              } catch (InterruptedException e) {
                log.error("VFS Retry Interrupted", e);
                throw new RuntimeException(e);
              }

            }

            // There is a chance that the listener was removed from the top level directory or
            // its children if they were deleted within some time window. Re-add files to be
            // monitored. The Monitor will ignore files that are already/still being monitored.
            // forEachCatchRTEs will capture a stream of thrown exceptions.
            // and can collect them to list or reduce into one exception

            forEachCatchRTEs(Arrays.stream(files), o -> {
              addFileToMonitor(o);
              log.debug("monitoring {}", o);
            });
          }

          log.debug("Rebuilding dynamic classloader using files- {}", stringify(files));

          VFSClassLoader cl;
          if (preDelegate) {
            cl = new VFSClassLoader(files, vfs, parent.getClassLoader());
          } else {
            cl = new PostDelegatingVFSClassLoader(files, vfs, parent.getClassLoader());
          }
          updateClassloader(files, cl);
          return;
        } catch (Exception e) {
          log.error("{}", e.getMessage(), e);
          try {
            Thread.sleep(DEFAULT_TIMEOUT);
          } catch (InterruptedException ie) {
            log.error("{}", ie.getMessage(), ie);
          }
        }
      }
    }
  };

  public String stringify(FileObject[] files) {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    String delim = "";
    for (FileObject file : files) {
      sb.append(delim);
      delim = ", ";
      sb.append(file.getName());
    }
    sb.append(']');
    return sb.toString();
  }

  @Override
  public synchronized ClassLoader getClassLoader() {
    if (cl.getParent() != parent.getClassLoader()) {
      scheduleRefresh();
    }
    return cl;
  }

  private void scheduleRefresh() {
    try {
      executor.execute(refresher);
    } catch (RejectedExecutionException e) {
      log.trace("Ignoring refresh request (already refreshing)");
    }
  }

  private synchronized void updateClassloader(FileObject[] files, VFSClassLoader cl) {
    this.files = files;
    this.cl = cl;
  }

  public AccumuloReloadingVFSClassLoader(String uris, FileSystemManager vfs,
      ReloadingClassLoader parent, long monitorDelay, boolean preDelegate)
      throws FileSystemException {

    this.uris = uris;
    this.parent = parent;
    this.preDelegate = preDelegate;

    ArrayList<FileObject> pathsToMonitor = new ArrayList<>();
    files = AccumuloVFSClassLoader.resolve(vfs, uris, pathsToMonitor);

    if (preDelegate) {
      cl = new VFSClassLoader(files, vfs, parent.getClassLoader()) {
        @Override
        public String getName() {
          return "AccumuloReloadingVFSClassLoader (loads everything defined by general.dynamic.classpaths)";
        }
      };
    } else {
      cl = new PostDelegatingVFSClassLoader(files, vfs, parent.getClassLoader()) {
        @Override
        public String getName() {
          return "AccumuloReloadingVFSClassLoader (loads everything defined by general.dynamic.classpaths)";
        }
      };
    }

    monitor = new DefaultFileMonitor(this);
    monitor.setDelay(monitorDelay);
    monitor.setRecursive(false);

    forEachCatchRTEs(pathsToMonitor.stream(), o -> {
      addFileToMonitor(o);
      log.debug("monitoring {}", o);
    });

    monitor.start();
  }

  private void addFileToMonitor(FileObject file) throws RuntimeException {
    try {
      if (monitor != null) {
        monitor.addFile(file);
      }
    } catch (RuntimeException re) {
      if (re.getMessage().contains("files-cache")) {
        log.error("files-cache error adding {} to VFS monitor. "
            + "There is no implementation for files-cache in VFS2", file, re);
      } else {
        log.error("Runtime error adding {} to VFS monitor", file, re);
      }

      throw re;
    }
  }

  private void removeFile(FileObject file) throws RuntimeException {
    try {
      if (monitor != null) {
        monitor.removeFile(file);
      }
    } catch (RuntimeException re) {
      log.error("Error removing file from VFS cache {}", file, re);
      throw re;
    }
  }

  public static <T> void forEachCatchRTEs(Stream<T> stream, Consumer<T> consumer) {
    stream.flatMap(o -> {
      try {
        consumer.accept(o);
        return null;
      } catch (RuntimeException e) {
        return Stream.of(e);
      }
    }).reduce((e1, e2) -> {
      e1.addSuppressed(e2);
      return e1;
    }).ifPresent(e -> {
      throw e;
    });
  }

  public AccumuloReloadingVFSClassLoader(String uris, FileSystemManager vfs,
      final ReloadingClassLoader parent, boolean preDelegate) throws FileSystemException {
    this(uris, vfs, parent, DEFAULT_TIMEOUT, preDelegate);
  }

  /**
   * Should be ok if this is not called because the thread started by DefaultFileMonitor is a daemon
   * thread
   */
  public void close() {

    forEachCatchRTEs(Stream.of(files), o -> {
      removeFile(o);
      log.debug("Removing file from monitoring {}", o);
    });

    executor.shutdownNow();
    monitor.stop();
  }

  @Override
  public void fileCreated(FileChangeEvent event) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("{} created, recreating classloader", event.getFileObject().getURL());
    }
    scheduleRefresh();
  }

  @Override
  public void fileDeleted(FileChangeEvent event) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("{} deleted, recreating classloader", event.getFileObject().getURL());
    }
    scheduleRefresh();
  }

  @Override
  public void fileChanged(FileChangeEvent event) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("{} changed, recreating classloader", event.getFileObject().getURL());
    }
    scheduleRefresh();
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();

    for (FileObject f : files) {
      try {
        buf.append("\t").append(f.getURL()).append("\n");
      } catch (FileSystemException e) {
        log.error("Error getting URL for file", e);
      }
    }

    return buf.toString();
  }

  // VisibleForTesting intentionally not using annotation from Guava because it adds unwanted
  // dependency
  void setMaxRetries(long maxRetries) {
    this.maxRetries = maxRetries;
  }

  private boolean retryPermitted(long retries) {
    return (maxRetries < 0 || retries < maxRetries);
  }
}
