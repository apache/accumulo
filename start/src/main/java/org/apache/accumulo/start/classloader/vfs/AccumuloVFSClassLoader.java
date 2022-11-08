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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.FileContentInfoFilenameFactory;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.commons.vfs2.provider.FileReplicator;
import org.apache.commons.vfs2.provider.hdfs.HdfsFileObject;
import org.apache.commons.vfs2.provider.hdfs.HdfsFileProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class builds a hierarchy of Classloaders in the form of:
 *
 * <pre>
 * SystemClassLoader that loads JVM classes
 *       ^
 *       |
 * AccumuloClassLoader loads jars from locations in general.classpaths.
 * Usually the URLs for HADOOP_HOME, ZOOKEEPER_HOME, ACCUMULO_HOME and their associated directories
 *       ^
 *       |
 * VFSClassLoader that loads jars from locations in general.vfs.classpaths.
 * Can be used to load accumulo jar from HDFS
 *       ^
 *       |
 * AccumuloReloadingVFSClassLoader That loads jars from locations in general.dynamic.classpaths.
 * Used to load jar dynamically.
 * </pre>
 */
@Deprecated
public class AccumuloVFSClassLoader {

  public static class AccumuloVFSClassLoaderShutdownThread implements Runnable {

    @Override
    public void run() {
      try {
        AccumuloVFSClassLoader.close();
      } catch (Exception e) {
        // do nothing, we are shutting down anyway
      }
    }

  }

  private static List<WeakReference<DefaultFileSystemManager>> vfsInstances =
      Collections.synchronizedList(new ArrayList<>());

  public static final String DYNAMIC_CLASSPATH_PROPERTY_NAME = "general.dynamic.classpaths";

  public static final String DEFAULT_DYNAMIC_CLASSPATH_VALUE = "";

  public static final String VFS_CLASSLOADER_SYSTEM_CLASSPATH_PROPERTY = "general.vfs.classpaths";

  public static final String VFS_CONTEXT_CLASSPATH_PROPERTY = "general.vfs.context.classpath.";

  public static final String VFS_CACHE_DIR = "general.vfs.cache.dir";

  private static ClassLoader parent = null;
  private static volatile ReloadingClassLoader loader = null;
  private static final Object lock = new Object();

  private static ContextManager contextManager;

  private static final Logger log = LoggerFactory.getLogger(AccumuloVFSClassLoader.class);

  static {
    // Register the shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(new AccumuloVFSClassLoaderShutdownThread()));
  }

  static FileObject[] resolve(FileSystemManager vfs, String uris) throws FileSystemException {
    return resolve(vfs, uris, new ArrayList<>());
  }

  static FileObject[] resolve(FileSystemManager vfs, String uris,
      ArrayList<FileObject> pathsToMonitor) throws FileSystemException {
    if (uris == null) {
      return new FileObject[0];
    }

    ArrayList<FileObject> classpath = new ArrayList<>();

    pathsToMonitor.clear();

    for (String path : uris.split(",")) {

      path = path.trim();

      if (path.equals("")) {
        continue;
      }

      path = AccumuloClassLoader.replaceEnvVars(path, System.getenv());

      log.debug("Resolving path element: {}", path);
      FileObject fo = vfs.resolveFile(path);

      switch (fo.getType()) {
        case FILE:
        case FOLDER:
          classpath.add(fo);
          pathsToMonitor.add(fo);
          break;
        case IMAGINARY:
          // assume it's a pattern
          var pattern = Pattern.compile(fo.getName().getBaseName());
          if (fo.getParent() != null) {
            // still monitor the parent
            pathsToMonitor.add(fo.getParent());
            if (fo.getParent().getType() == FileType.FOLDER) {
              FileObject[] children = fo.getParent().getChildren();
              for (FileObject child : children) {
                if (child.getType() == FileType.FILE
                    && pattern.matcher(child.getName().getBaseName()).matches()) {
                  classpath.add(child);
                }
              }
            } else {
              log.debug("classpath entry " + fo.getParent() + " is " + fo.getParent().getType());
            }
          } else {
            log.warn("ignoring classpath entry {}", fo);
          }
          break;
        default:
          log.warn("ignoring classpath entry {}", fo);
          break;
      }

    }

    return classpath.toArray(new FileObject[classpath.size()]);
  }

  private static ReloadingClassLoader createDynamicClassloader(final ClassLoader parent)
      throws IOException {
    String dynamicCPath = AccumuloClassLoader.getAccumuloProperty(DYNAMIC_CLASSPATH_PROPERTY_NAME,
        DEFAULT_DYNAMIC_CLASSPATH_VALUE);

    ReloadingClassLoader wrapper = () -> parent;

    if (dynamicCPath == null || dynamicCPath.equals("")) {
      return wrapper;
    }

    // TODO monitor time for lib/ext was 1 sec... should this be configurable? - ACCUMULO-1301
    return new AccumuloReloadingVFSClassLoader(dynamicCPath, generateVfs(), wrapper, 1000, true);
  }

  public static ClassLoader getClassLoader() {
    try {
      return getClassLoader_Internal();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static ClassLoader getClassLoader_Internal() throws IOException {
    ReloadingClassLoader localLoader = loader;
    while (localLoader == null) {
      synchronized (lock) {
        if (loader == null) {

          FileSystemManager vfs = generateVfs();

          // Set up the 2nd tier class loader
          if (parent == null) {
            parent = AccumuloClassLoader.getClassLoader();
          }

          FileObject[] vfsCP = resolve(vfs, AccumuloClassLoader
              .getAccumuloProperty(VFS_CLASSLOADER_SYSTEM_CLASSPATH_PROPERTY, ""));

          if (vfsCP.length == 0) {
            localLoader = createDynamicClassloader(parent);
            loader = localLoader;
            return localLoader.getClassLoader();
          }

          // Create the Accumulo Context ClassLoader using the DEFAULT_CONTEXT
          localLoader = createDynamicClassloader(new VFSClassLoader(vfsCP, vfs, parent));
          loader = localLoader;

          // An HDFS FileSystem and Configuration object were created for each unique HDFS namespace
          // in the call to resolve above.
          // The HDFS Client did us a favor and cached these objects so that the next time someone
          // calls FileSystem.get(uri), they
          // get the cached object. However, these objects were created not with the system VFS
          // classloader, but the classloader above
          // it. We need to override the classloader on the Configuration objects. Ran into an issue
          // were log recovery was being attempted
          // and SequenceFile$Reader was trying to instantiate the key class via
          // WritableName.getClass(String, Configuration)
          for (FileObject fo : vfsCP) {
            if (fo instanceof HdfsFileObject) {
              String uri = fo.getName().getRootURI();
              Configuration c = new Configuration(true);
              c.set(FileSystem.FS_DEFAULT_NAME_KEY, uri);
              FileSystem fs = FileSystem.get(c);
              fs.getConf().setClassLoader(loader.getClassLoader());
            }
          }

        }
      }
    }

    return localLoader.getClassLoader();
  }

  public static FileSystemManager generateVfs() throws FileSystemException {
    DefaultFileSystemManager vfs = new DefaultFileSystemManager();
    vfs.addProvider("res", new org.apache.commons.vfs2.provider.res.ResourceFileProvider());
    vfs.addProvider("zip", new org.apache.commons.vfs2.provider.zip.ZipFileProvider());
    vfs.addProvider("gz", new org.apache.commons.vfs2.provider.gzip.GzipFileProvider());
    vfs.addProvider("ram", new org.apache.commons.vfs2.provider.ram.RamFileProvider());
    vfs.addProvider("file", new org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider());
    vfs.addProvider("jar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("http", new org.apache.commons.vfs2.provider.http.HttpFileProvider());
    vfs.addProvider("https", new org.apache.commons.vfs2.provider.https.HttpsFileProvider());
    vfs.addProvider("ftp", new org.apache.commons.vfs2.provider.ftp.FtpFileProvider());
    vfs.addProvider("ftps", new org.apache.commons.vfs2.provider.ftps.FtpsFileProvider());
    vfs.addProvider("war", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("par", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("ear", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("sar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("ejb3", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
    vfs.addProvider("tmp", new org.apache.commons.vfs2.provider.temp.TemporaryFileProvider());
    vfs.addProvider("tar", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
    vfs.addProvider("tbz2", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
    vfs.addProvider("tgz", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
    vfs.addProvider("bz2", new org.apache.commons.vfs2.provider.bzip2.Bzip2FileProvider());
    vfs.addProvider("hdfs", new HdfsFileProvider());
    vfs.addExtensionMap("jar", "jar");
    vfs.addExtensionMap("zip", "zip");
    vfs.addExtensionMap("gz", "gz");
    vfs.addExtensionMap("tar", "tar");
    vfs.addExtensionMap("tbz2", "tar");
    vfs.addExtensionMap("tgz", "tar");
    vfs.addExtensionMap("bz2", "bz2");
    vfs.addMimeTypeMap("application/java-archive", "jar");
    vfs.addMimeTypeMap("application/x-tar", "tar");
    vfs.addMimeTypeMap("application/x-gzip", "gz");
    vfs.addMimeTypeMap("application/zip", "zip");
    vfs.setFileContentInfoFactory(new FileContentInfoFilenameFactory());
    vfs.setFilesCache(new SoftRefFilesCache());
    File cacheDir = computeTopCacheDir();
    vfs.setReplicator(new UniqueFileReplicator(cacheDir));
    vfs.setCacheStrategy(CacheStrategy.ON_RESOLVE);
    vfs.init();
    vfsInstances.add(new WeakReference<>(vfs));
    return vfs;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "tmpdir is controlled by admin, not unchecked user input")
  private static File computeTopCacheDir() {
    String cacheDirPath = AccumuloClassLoader.getAccumuloProperty(VFS_CACHE_DIR,
        System.getProperty("java.io.tmpdir"));
    String procName = ManagementFactory.getRuntimeMXBean().getName();
    return new File(cacheDirPath,
        "accumulo-vfs-cache-" + procName + "-" + System.getProperty("user.name", "nouser"));
  }

  public interface Printer {
    void print(String s);
  }

  public static void printClassPath(boolean debug) {
    printClassPath(System.out::print, debug);
  }

  public static String getClassPath(boolean debug) {
    StringBuilder cp = new StringBuilder();
    printClassPath(cp::append, debug);
    return cp.toString();
  }

  private static void printJar(Printer out, String jarPath, boolean debug, boolean sawFirst) {
    if (debug) {
      out.print("\t");
    }
    if (!debug && sawFirst) {
      out.print(":");
    }
    out.print(jarPath);
    if (debug) {
      out.print("\n");
    }
  }

  public static void printClassPath(Printer out, boolean debug) {
    try {
      ClassLoader cl = getClassLoader();
      ArrayList<ClassLoader> classloaders = new ArrayList<>();

      while (cl != null) {
        classloaders.add(cl);
        cl = cl.getParent();
      }

      Collections.reverse(classloaders);

      int level = 0;

      for (ClassLoader classLoader : classloaders) {

        level++;

        if (debug && level > 1) {
          out.print("\n");
        }
        if (!debug && level < 2) {
          continue;
        }

        boolean sawFirst = false;
        String classLoaderDescription = "Level: " + level + ", Name: " + classLoader.getName()
            + ", class: " + classLoader.getClass().getName();
        if (classLoader.getClass().getName().startsWith("jdk.internal")) {
          if (debug) {
            out.print(classLoaderDescription + ": configuration not inspectable.\n");
          }
        } else if (classLoader instanceof URLClassLoader) {
          if (debug) {
            out.print(classLoaderDescription + ": URL classpath items are:\n");
          }
          for (URL u : ((URLClassLoader) classLoader).getURLs()) {
            printJar(out, u.getFile(), debug, sawFirst);
            sawFirst = true;
          }
        } else if (classLoader instanceof VFSClassLoader) {
          if (debug) {
            out.print(classLoaderDescription + ": VFS classpaths items are:\n");
          }
          VFSClassLoader vcl = (VFSClassLoader) classLoader;
          for (FileObject f : vcl.getFileObjects()) {
            printJar(out, f.getURL().getFile(), debug, sawFirst);
            sawFirst = true;
          }
        } else {
          if (debug) {
            out.print(
                classLoaderDescription + ": Unknown classloader: " + classLoader.getClass() + "\n");
          }
        }
      }
      out.print("\n");
    } catch (Exception t) {
      throw new RuntimeException(t);
    }
  }

  public static void setContextConfig(Supplier<Map<String,String>> contextConfigSupplier) {
    var config = new ContextManager.DefaultContextsConfig(contextConfigSupplier);
    try {
      getContextManager().setContextConfig(config);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static void removeUnusedContexts(Set<String> contextsInUse) {
    try {
      getContextManager().removeUnusedContexts(contextsInUse);
    } catch (IOException e) {
      log.warn("{}", e.getMessage(), e);
    }
  }

  public static ClassLoader getContextClassLoader(String contextName) {
    try {
      return getContextManager().getClassLoader(contextName);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Error getting context class loader for context: " + contextName, e);
    }
  }

  private static synchronized ContextManager getContextManager() throws IOException {
    if (contextManager == null) {
      getClassLoader();
      contextManager = new ContextManager(generateVfs(), AccumuloVFSClassLoader::getClassLoader);
    }

    return contextManager;
  }

  public static void close() {
    for (WeakReference<DefaultFileSystemManager> vfsInstance : vfsInstances) {
      DefaultFileSystemManager ref = vfsInstance.get();
      if (ref != null) {
        FileReplicator replicator;
        try {
          replicator = ref.getReplicator();
          if (replicator instanceof UniqueFileReplicator) {
            ((UniqueFileReplicator) replicator).close();
          }
        } catch (FileSystemException e) {
          log.error("FileSystemException", e);
        }
        ref.close();
      }
    }
    try {
      FileUtils.deleteDirectory(computeTopCacheDir());
    } catch (IOException e) {
      log.error("IOException", e);
    }
  }
}
