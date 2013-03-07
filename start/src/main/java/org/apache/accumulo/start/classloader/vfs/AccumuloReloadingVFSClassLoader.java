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

import java.util.ArrayList;

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
public class AccumuloReloadingVFSClassLoader implements FileListener, ReloadingClassLoader {
  
  private static final Logger log = Logger.getLogger(AccumuloReloadingVFSClassLoader.class);

  /* 5 minute timeout */
  private static final int DEFAULT_TIMEOUT = 300000;
  
  private String uris;
  private FileObject[] files;
  private FileSystemManager vfs = null;
  private ReloadingClassLoader parent = null;
  private DefaultFileMonitor monitor = null;
  private VFSClassLoader cl = null;
  private boolean preDelegate;

  @Override
  public synchronized ClassLoader getClassLoader() {
    if (cl == null || cl.getParent() != parent.getClassLoader()) {
      try {
        files = AccumuloVFSClassLoader.resolve(vfs, uris);
        
        if (preDelegate)
          cl = new VFSClassLoader(files, vfs, parent.getClassLoader());
        else
          cl = new PostDelegatingVFSClassLoader(files, vfs, parent.getClassLoader());

      } catch (FileSystemException fse) {
        throw new RuntimeException(fse);
      }
    }
    
    return cl;
  }
  
  private synchronized void setClassloader(VFSClassLoader cl) {
    this.cl = cl;
    
  }

  public AccumuloReloadingVFSClassLoader(String uris, FileSystemManager vfs, ReloadingClassLoader parent, long monitorDelay, boolean preDelegate)
      throws FileSystemException {

    this.uris = uris;
    this.vfs = vfs;
    this.parent = parent;
    this.preDelegate = preDelegate;
    
    ArrayList<FileObject> pathsToMonitor = new ArrayList<FileObject>();
    files = AccumuloVFSClassLoader.resolve(vfs, uris, pathsToMonitor);

    if (preDelegate)
      cl = new VFSClassLoader(files, vfs, parent.getClassLoader());
    else
      cl = new PostDelegatingVFSClassLoader(files, vfs, parent.getClassLoader());
    
    monitor = new DefaultFileMonitor(this);
    monitor.setDelay(monitorDelay);
    monitor.setRecursive(false);
    for (FileObject file : pathsToMonitor) {
      monitor.addFile(file);
      log.debug("monitoring " + file);
    }
    monitor.start();
  }
  
  public AccumuloReloadingVFSClassLoader(String uris, FileSystemManager vfs, final ReloadingClassLoader parent, boolean preDelegate)
      throws FileSystemException {
    this(uris, vfs, parent, DEFAULT_TIMEOUT, preDelegate);
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
