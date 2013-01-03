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
package org.apache.accumulo.start.classloader.vfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.SecureClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.log4j.Logger;

/**
 * Classloader that delegates operations to a VFSClassLoader object. This class also listens
 * for changes in any of the files/directories that are in the classpath and will recreate
 * the delegate object if there is any change in the classpath.
 *
 */
public class AccumuloReloadingVFSClassLoader extends SecureClassLoader implements FileListener {
  
  private static final Logger log = Logger.getLogger(AccumuloReloadingVFSClassLoader.class);

  /* 5 minute timeout */
  private static final int DEFAULT_TIMEOUT = 300000;
  
  private String uris;
  private FileObject[] files;
  private FileSystemManager vfs = null;
  private ClassLoader parent = null;
  private DefaultFileMonitor monitor = null;
  private volatile VFSClassLoader cl = null;

  private synchronized VFSClassLoader getClassloader() {
    if (cl == null) {
      try {
        files = AccumuloVFSClassLoader.resolve(vfs, uris);
        
        if (null != parent)
          setClassloader(new VFSClassLoader(files, vfs, parent));
        else
          setClassloader(new VFSClassLoader(files, vfs));

      } catch (FileSystemException fse) {
        throw new RuntimeException(fse);
      }
    }
    
    return cl;
  }
  
  private synchronized void setClassloader(VFSClassLoader cl) {
    this.cl = cl;
    
  }

  public AccumuloReloadingVFSClassLoader(String uris, FileSystemManager vfs, ClassLoader parent, long monitorDelay) throws FileSystemException {
    super(parent);
    
    this.uris = uris;
    this.vfs = vfs;
    this.parent = parent;
    
    ArrayList<FileObject> pathsToMonitor = new ArrayList<FileObject>();
    files = AccumuloVFSClassLoader.resolve(vfs, uris, pathsToMonitor);

    if (null != parent)
      cl = new VFSClassLoader(files, vfs, parent);
    else
      cl = new VFSClassLoader(files, vfs);
    
    monitor = new DefaultFileMonitor(this);
    monitor.setDelay(monitorDelay);
    monitor.setRecursive(false);
    for (FileObject file : pathsToMonitor) {
      monitor.addFile(file);
      log.debug("monitoring " + file);
    }
    monitor.start();
  }
  
  public AccumuloReloadingVFSClassLoader(String uris, FileSystemManager vfs, ClassLoader parent) throws FileSystemException {
    this(uris, vfs, parent, DEFAULT_TIMEOUT);
  }

  public FileObject[] getFiles() {
    return this.files;
  }
  
  /**
   * Should be ok if this is not called because the thread started by DefaultFileMonitor is a daemon thread
   */
  public void close() {
    monitor.stop();
  }

  public void fileCreated(FileChangeEvent event) throws Exception {
    if (log.isDebugEnabled())
      log.debug(event.getFile().getURL().toString() + " created, recreating classloader");
    setClassloader(null);
  }

  public void fileDeleted(FileChangeEvent event) throws Exception {
    if (log.isDebugEnabled())
      log.debug(event.getFile().getURL().toString() + " changed, recreating classloader");
    setClassloader(null);
  }

  public void fileChanged(FileChangeEvent event) throws Exception {
    if (log.isDebugEnabled())
      log.debug(event.getFile().getURL().toString() + " deleted, recreating classloader");
    setClassloader(null);
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
     return this.getClassloader().loadClass(name);
  }
  
  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    return this.loadClass(name);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    return this.loadClass(name);
  }

  @Override
  public URL getResource(String name) {
    return this.getClassloader().getResource(name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    return this.getClassloader().getResources(name);
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    return this.getClassloader().getResourceAsStream(name);
  }

  @Override
  public synchronized void setDefaultAssertionStatus(boolean enabled) {
    this.getClassloader().setDefaultAssertionStatus(enabled);
  }

  @Override
  public synchronized void setPackageAssertionStatus(String packageName, boolean enabled) {
    this.getClassloader().setPackageAssertionStatus(packageName, enabled);
  }

  @Override
  public synchronized void setClassAssertionStatus(String className, boolean enabled) {
    this.getClassloader().setClassAssertionStatus(className, enabled);
  }

  @Override
  public synchronized void clearAssertionStatus() {
    this.getClassloader().clearAssertionStatus();
  }
  
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    
    for (FileObject f : files) {
      try {
        buf.append("\t").append(f.getURL().toString()).append("\n");
      } catch (FileSystemException e) {
        log.error("Error getting URL for file", e);
      }
    }
    
    return buf.toString();
  }


  
}
