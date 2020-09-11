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
package org.apache.accumulo.classloader.vfs;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.commons.vfs2.provider.hdfs.HdfsFileObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A {@code ClassLoader} implementation that watches for changes in any of the files/directories in
 * the classpath. When a change is noticed, this classloader will then load the new classes in
 * subsequent calls to loadClass. This classloader supports both the normal classloader
 * pre-delegation model and a post-delegation model. To enable the post-delegation feature set the
 * environment variable <b>vfs.class.loader.delegation</b> to "post".
 * 
 * <p>
 * This classloader uses the following environment variables:
 * 
 * <ol>
 * <li><b>vfs.cache.dir</b> - for specifying the directory to use for the local VFS cache (default
 * is the system property <b>java.io.tmpdir</b></li>
 * <li><b>vfs.classpath.monitor.seconds</b> - for specifying the file system monitor (default:
 * 5m)</li>
 * <li><b>vfs.class.loader.classpath</b> - for specifying the class path</li>
 * <li><b>vfs.class.loader.delegation</b> - valid values are "pre" and "post" (default: pre)</li>
 * </ol>
 *
 * <p>
 * This class will attempt to perform substitution on any environment variables found in the values.
 * For example, the environment variable <b>vfs.cache.dir</b> can be set to <b>$HOME/cache</b>.
 */
public class ReloadingVFSClassLoader extends ClassLoader implements Closeable, FileListener {

  public static final String VFS_CLASSPATH_MONITOR_INTERVAL = "vfs.classpath.monitor.seconds";
  public static final String VFS_CACHE_DIR_PROPERTY = "vfs.cache.dir";
  public static final String VFS_CLASSLOADER_CLASSPATH = "vfs.class.loader.classpath";
  public static final String VFS_CLASSLOADER_DELEGATION = "vfs.class.loader.delegation";

  private static final String VFS_CACHE_DIR_DEFAULT = "java.io.tmpdir";
  private static final Logger LOG = LoggerFactory.getLogger(ReloadingVFSClassLoader.class);

  // set to 5 mins. The rationale behind this large time is to avoid a gazillion tservers all asking
  // the name node for info too frequently.
  private static final long DEFAULT_TIMEOUT = TimeUnit.MINUTES.toMillis(5);

  private volatile long maxWaitInterval = 60000;
  private volatile long maxRetries = -1;
  private volatile long sleepInterval = 5000;

  private final DefaultFileMonitor monitor;
  private final ThreadPoolExecutor executor;
  private final ClassLoader parent;
  private final ReentrantReadWriteLock updateLock = new ReentrantReadWriteLock(true);
  private final String name;
  private final String classPath;
  private final Boolean preDelegation;
  private final FileSystemManager vfs;
  private FileObject[] files;
  private VFSClassLoaderWrapper cl = null;

  {
    classPath = getClassPath();
    preDelegation = isPreDelegationModel();

    try {
      vfs = VFSManager.generateVfs();
    } catch (FileSystemException e) {
      throw new RuntimeException("Error creating VFS", e);
    }

    BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(2);
    ThreadFactory factory = r -> {
      Thread t = new Thread(r);
      t.setDaemon(true);
      return t;
    };
    executor = new ThreadPoolExecutor(1, 1, 1, SECONDS, queue, factory);
  }

  FileSystemManager getFileSystemManager() {
    return vfs;
  }

  /**
   * Get the classpath value from the environment and resolve embedded env vars
   * 
   * @return classpath value
   */
  protected String getClassPath() {
    String cp = System.getenv(VFS_CLASSLOADER_CLASSPATH);
    if (null == cp || cp.isBlank()) {
      throw new RuntimeException(VFS_CLASSLOADER_CLASSPATH + " must be set in the environment");
    }
    String result = replaceEnvVars(cp, System.getenv());
    LOG.debug("Classpath set to: {}", result);
    return result;
  }

  /**
   * Get the delegation model
   * 
   * @return true if pre delegaion, false if post delegation
   */
  protected boolean isPreDelegationModel() {
    String delegation = System.getenv(VFS_CLASSLOADER_DELEGATION);
    boolean preDelegation = true;
    if (null != delegation && delegation.equalsIgnoreCase("post")) {
      preDelegation = false;
    }
    LOG.debug("ClassLoader configured for pre-delegation: {}", preDelegation);
    return preDelegation;
  }

  /**
   * Get the directory for the VFS cache
   * 
   * @return VFS cache directory
   */
  static String getVFSCacheDir() {
    // Get configuration properties from the environment variables
    String vfsCacheDir = System.getenv(VFS_CACHE_DIR_PROPERTY);
    if (null == vfsCacheDir || vfsCacheDir.isBlank()) {
      vfsCacheDir = System.getProperty(VFS_CACHE_DIR_DEFAULT);
    }
    String cache = replaceEnvVars(vfsCacheDir, System.getenv());
    // LOG.debug("VFS Cache Dir set to: {}", cache);
    return cache;
  }

  /**
   * Replace environment variables in the string with their actual value
   */
  static String replaceEnvVars(String classpath, Map<String,String> env) {
    Pattern envPat = Pattern.compile("\\$[A-Za-z][a-zA-Z0-9_]*");
    Matcher envMatcher = envPat.matcher(classpath);
    while (envMatcher.find(0)) {
      // name comes after the '$'
      String varName = envMatcher.group().substring(1);
      String varValue = env.get(varName);
      if (varValue == null) {
        varValue = "";
      }
      classpath = (classpath.substring(0, envMatcher.start()) + varValue
          + classpath.substring(envMatcher.end()));
      envMatcher.reset(classpath);
    }
    return classpath;
  }

  /**
   * Get the file system monitor interval
   * 
   * @return monitor interval in ms
   */
  protected long getMonitorInterval() {
    String interval = System.getenv(VFS_CLASSPATH_MONITOR_INTERVAL);
    if (null != interval && !interval.isBlank()) {
      try {
        return TimeUnit.SECONDS.toMillis(Long.parseLong(interval));
      } catch (NumberFormatException e) {
        return DEFAULT_TIMEOUT;
      }
    }
    return DEFAULT_TIMEOUT;
  }

  /**
   * This task replaces the delegate classloader with a new instance when the filesystem has
   * changed. This will orphan the old classloader and the only references to the old classloader
   * are from the objects that it loaded.
   */
  private final Runnable refresher = new Runnable() {
    @Override
    public void run() {
      while (!executor.isTerminating()) {
        try {
          FileObject[] files = VFSManager.resolve(getFileSystemManager(), classPath);

          long retries = 0;
          long currentSleepMillis = sleepInterval;

          if (files.length == 0) {
            while (files.length == 0 && retryPermitted(retries)) {

              try {
                LOG.debug("VFS path was empty.  Waiting " + currentSleepMillis + " ms to retry");
                Thread.sleep(currentSleepMillis);
                files = VFSManager.resolve(getFileSystemManager(), classPath);
                retries++;

                currentSleepMillis = Math.min(maxWaitInterval, currentSleepMillis + sleepInterval);

              } catch (InterruptedException e) {
                LOG.error("VFS Retry Interruped", e);
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
              LOG.debug("monitoring {}", o);
            });
          }

          LOG.debug("Rebuilding dynamic classloader using files- {}", stringify(files));

          VFSClassLoaderWrapper cl;
          if (preDelegation)
            // This is the normal classloader parent delegation model
            cl = new VFSClassLoaderWrapper(files, getFileSystemManager(), parent);
          else
            // This delegates to the parent after we lookup locally first.
            cl = new VFSClassLoaderWrapper(files, getFileSystemManager()) {
              @Override
              protected synchronized Class<?> loadClass(String name, boolean resolve)
                  throws ClassNotFoundException {
                Class<?> c = findLoadedClass(name);
                if (c != null)
                  return c;
                try {
                  // try finding this class here instead of parent
                  return findClass(name);
                } catch (ClassNotFoundException e) {

                }
                return super.loadClass(name, resolve);
              }
            };
          updateClassloader(files, cl);
          return;
        } catch (Exception e) {
          LOG.error("{}", e.getMessage(), e);
          try {
            Thread.sleep(getMonitorInterval());
          } catch (InterruptedException ie) {
            LOG.error("{}", ie.getMessage(), ie);
          }
        }
      }
    }
  };

  public ReloadingVFSClassLoader(ClassLoader parent) {
    super(ReloadingVFSClassLoader.class.getSimpleName(), parent);
    this.name = ReloadingVFSClassLoader.class.getSimpleName();
    this.parent = parent;

    ArrayList<FileObject> pathsToMonitor = new ArrayList<>();
    try {
      this.files = VFSManager.resolve(getFileSystemManager(), classPath, pathsToMonitor);
      this.cl = new VFSClassLoaderWrapper(files, getFileSystemManager(), parent);
    } catch (FileSystemException e) {
      throw new RuntimeException("Error creating classloader", e);
    }

    // An HDFS FileSystem and Configuration object were created for each unique HDFS namespace
    // in the call to resolve above. The HDFS Client did us a favor and cached these objects
    // so that the next time someone calls FileSystem.get(uri), they get the cached object.
    // However, these objects were created not with the system VFS classloader, but the
    // classloader above it. We need to override the classloader on the Configuration objects.
    // Ran into an issue were log recovery was being attempted and SequenceFile$Reader was
    // trying to instantiate the key class via WritableName.getClass(String, Configuration)
    for (FileObject fo : this.files) {
      if (fo instanceof HdfsFileObject) {
        String uri = fo.getName().getRootURI();
        Configuration c = new Configuration(true);
        c.set(FileSystem.FS_DEFAULT_NAME_KEY, uri);
        try {
          FileSystem fs = FileSystem.get(c);
          fs.getConf().setClassLoader(this.cl);
        } catch (IOException e) {
          throw new RuntimeException("Error setting classloader on HDFS FileSystem object", e);
        }
      }
    }

    this.monitor = new DefaultFileMonitor(this);
    monitor.setDelay(getMonitorInterval());
    monitor.setRecursive(false);
    LOG.debug("Monitor interval set to: {}", monitor.getDelay());

    forEachCatchRTEs(pathsToMonitor.stream(), o -> {
      addFileToMonitor(o);
      LOG.debug("monitoring {}", o);
    });
    monitor.start();
  }

  private void addFileToMonitor(FileObject file) throws RuntimeException {
    try {
      if (monitor != null)
        monitor.addFile(file);
    } catch (RuntimeException re) {
      if (re.getMessage().contains("files-cache"))
        LOG.error("files-cache error adding {} to VFS monitor. "
            + "There is no implementation for files-cache in VFS2", file, re);
      else
        LOG.error("Runtime error adding {} to VFS monitor", file, re);

      throw re;
    }
  }

  private synchronized void updateClassloader(FileObject[] files, VFSClassLoaderWrapper cl) {
    this.files = files;
    try {
      updateLock.writeLock().lock();
      this.cl = cl;
    } finally {
      updateLock.writeLock().unlock();
    }
  }

  private void removeFile(FileObject file) throws RuntimeException {
    try {
      if (monitor != null)
        monitor.removeFile(file);
    } catch (RuntimeException re) {
      LOG.error("Error removing file from VFS cache {}", file, re);
      throw re;
    }
  }

  @Override
  public void fileCreated(FileChangeEvent event) throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("{} created, recreating classloader", event.getFileObject().getURL());
    scheduleRefresh();
  }

  @Override
  public void fileDeleted(FileChangeEvent event) throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("{} deleted, recreating classloader", event.getFileObject().getURL());
    scheduleRefresh();
  }

  @Override
  public void fileChanged(FileChangeEvent event) throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("{} changed, recreating classloader", event.getFileObject().getURL());
    scheduleRefresh();
  }

  private void scheduleRefresh() {
    try {
      executor.execute(refresher);
    } catch (RejectedExecutionException e) {
      LOG.trace("Ignoring refresh request (already refreshing)");
    }
  }

  @Override
  public void close() {

    forEachCatchRTEs(Stream.of(files), o -> {
      removeFile(o);
      LOG.debug("Removing file from monitoring {}", o);
    });

    executor.shutdownNow();
    monitor.stop();

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

  // VisibleForTesting intentionally not using annotation from Guava
  // because it adds unwanted dependency
  void setMaxRetries(long maxRetries) {
    this.maxRetries = maxRetries;
  }

  private boolean retryPermitted(long retries) {
    return (maxRetries < 0 || retries < maxRetries);
  }

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

  private VFSClassLoaderWrapper getClassLoader() {
    try {
      updateLock.readLock().lock();
      return cl;
    } finally {
      updateLock.readLock().unlock();
    }
  }

  public VFSClassLoaderWrapper getWrapper() {
    return getClassLoader();
  }

  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    return getClassLoader().findClass(name);
  }

  @Override
  public URL findResource(String name) {
    return getClassLoader().findResource(name);
  }

  @Override
  public Enumeration<URL> findResources(String name) throws IOException {
    return getClassLoader().findResources(name);
  }

  @Override
  public int hashCode() {
    return getClassLoader().hashCode();
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();

    for (FileObject f : files) {
      try {
        buf.append("\t").append(f.getURL()).append("\n");
      } catch (FileSystemException e) {
        LOG.error("Error getting URL for file", e);
      }
    }
    return buf.toString();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return getClassLoader().loadClass(name);
  }

  @Override
  public URL getResource(String name) {
    return getClassLoader().getResource(name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    return getClassLoader().getResources(name);
  }

  @Override
  public Stream<URL> resources(String name) {
    return getClassLoader().resources(name);
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    return getClassLoader().getResourceAsStream(name);
  }

  @Override
  public void setDefaultAssertionStatus(boolean enabled) {
    getClassLoader().setDefaultAssertionStatus(enabled);
  }

  @Override
  public void setPackageAssertionStatus(String packageName, boolean enabled) {
    getClassLoader().setPackageAssertionStatus(packageName, enabled);
  }

  @Override
  public void setClassAssertionStatus(String className, boolean enabled) {
    getClassLoader().setClassAssertionStatus(className, enabled);
  }

  @Override
  public void clearAssertionStatus() {
    getClassLoader().clearAssertionStatus();
  }

}
