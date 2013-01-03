/**
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;

public class ContextManager {
  
  // there is a lock per context so that one context can initialize w/o blocking another context
  private class Context {
    AccumuloReloadingVFSClassLoader loader;
    String uris;
    boolean closed = false;
    
    Context(String uris) {
      this.uris = uris;
    }
    
    synchronized AccumuloReloadingVFSClassLoader getClassLoader() throws FileSystemException {
      if (closed)
        return null;
      
      if (loader == null) {
        loader = new AccumuloReloadingVFSClassLoader(uris, vfs, parent);
      }
      
      return loader;
    }
    
    synchronized void close() {
      closed = true;
      loader.close();
      loader = null;
    }
  }

  private Map<String,Context> contexts = new HashMap<String,Context>();

  private volatile ContextConfig config;
  private FileSystemManager vfs;
  private ClassLoader parent;
  
  ContextManager(FileSystemManager vfs, ClassLoader parent) {
    this.vfs = vfs;
    this.parent = parent;
  }
  
  public interface ContextConfig {
    String getContextURIs(String context);
    
    boolean isIsolated(String context);
  }
  
  /**
   * configuration must be injected for ContextManager to work
   * 
   * @param config
   */
  public synchronized void setContextConfig(ContextConfig config) {
    if (this.config != null)
      throw new IllegalStateException("Context manager config already set");
    this.config = config;
  }
  
  public ClassLoader getClassLoader(String contextName) throws FileSystemException {

    String uris = config.getContextURIs(contextName);
    
    if (uris == null)
      throw new IllegalArgumentException("Unknown context " + contextName);
    
    Context context = null;
    Context contextToClose = null;
    
    synchronized (this) {
      // only manipulate internal data structs in this sync block... avoid creating or closing classloader, reading config, etc... basically avoid operations
      // that may block
      context = contexts.get(context);
      
      if (context == null) {
        context = new Context(uris);
        contexts.put(contextName, context);
      } else if (!context.uris.equals(uris)) {
        contextToClose = context;
        context = new Context(uris);
        contexts.put(contextName, context);
      }
    }
    
    if (contextToClose != null)
      contextToClose.close();

    AccumuloReloadingVFSClassLoader loader = context.getClassLoader();
    if (loader == null) {
      // ooppss, context was closed by another thread, try again
      return getClassLoader(contextName);
    }
    
    return loader;

  }
  
  public <U> Class<? extends U> loadClass(String context, String classname, Class<U> extension) throws ClassNotFoundException {
    try {
      return (Class<? extends U>) getClassLoader(context).loadClass(classname).asSubclass(extension);
    } catch (IOException e) {
      throw new ClassNotFoundException("IO Error loading class " + classname, e);
    }
  }
  
  public void removeUnusedContexts(Set<String> inUse) {
    
    Map<String,Context> unused;
    
    synchronized (this) {
      unused = new HashMap<String,Context>(contexts);
      unused.keySet().removeAll(inUse);
      contexts.keySet().removeAll(unused.keySet());
    }
    
    for (Context context : unused.values()) {
      // close outside of lock
      context.close();
    }
  }
}
