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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextManager {

  private static final Logger log = LoggerFactory.getLogger(ContextManager.class);

  // there is a lock per context so that one context can initialize w/o blocking another context
  private class Context {
    AccumuloReloadingVFSClassLoader loader;
    ContextConfig cconfig;
    boolean closed = false;

    Context(ContextConfig cconfig) {
      this.cconfig = cconfig;
    }

    synchronized ClassLoader getClassLoader() throws FileSystemException {
      if (closed)
        return null;

      if (loader == null) {
        loader = new AccumuloReloadingVFSClassLoader(cconfig.uris, vfs, parent, cconfig.preDelegation);
      }

      return loader.getClassLoader();
    }

    synchronized void close() {
      closed = true;
      if (loader != null) {
        loader.close();
      }
      loader = null;
    }
  }

  private Map<String,Context> contexts = new HashMap<>();

  private volatile ContextsConfig config;
  private FileSystemManager vfs;
  private ReloadingClassLoader parent;

  ContextManager(FileSystemManager vfs, ReloadingClassLoader parent) {
    this.vfs = vfs;
    this.parent = parent;
  }

  public static class ContextConfig {
    String uris;
    boolean preDelegation;

    public ContextConfig(String uris, boolean preDelegation) {
      this.uris = uris;
      this.preDelegation = preDelegation;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ContextConfig) {
        ContextConfig oc = (ContextConfig) o;

        return uris.equals(oc.uris) && preDelegation == oc.preDelegation;
      }

      return false;
    }

    @Override
    public int hashCode() {
      return uris.hashCode() + (preDelegation ? Boolean.TRUE : Boolean.FALSE).hashCode();
    }
  }

  public interface ContextsConfig {
    ContextConfig getContextConfig(String context);
  }

  public static class DefaultContextsConfig implements ContextsConfig {

    private Iterable<Entry<String,String>> config;

    public DefaultContextsConfig(Iterable<Entry<String,String>> config) {
      this.config = config;
    }

    @Override
    public ContextConfig getContextConfig(String context) {

      String key = AccumuloVFSClassLoader.VFS_CONTEXT_CLASSPATH_PROPERTY + context;

      String uris = null;
      boolean preDelegate = true;

      Iterator<Entry<String,String>> iter = config.iterator();
      while (iter.hasNext()) {
        Entry<String,String> entry = iter.next();
        if (entry.getKey().equals(key)) {
          uris = entry.getValue();
        }

        if (entry.getKey().equals(key + ".delegation") && entry.getValue().trim().equalsIgnoreCase("post")) {
          preDelegate = false;
        }
      }

      if (uris != null)
        return new ContextConfig(uris, preDelegate);

      return null;
    }
  }

  /**
   * configuration must be injected for ContextManager to work
   */
  public synchronized void setContextConfig(ContextsConfig config) {
    if (this.config != null)
      throw new IllegalStateException("Context manager config already set");
    this.config = config;
  }

  public ClassLoader getClassLoader(String contextName) throws FileSystemException {

    ContextConfig cconfig = config.getContextConfig(contextName);

    if (cconfig == null)
      throw new IllegalArgumentException("Unknown context " + contextName);

    Context context = null;
    Context contextToClose = null;

    synchronized (this) {
      // only manipulate internal data structs in this sync block... avoid creating or closing classloader, reading config, etc... basically avoid operations
      // that may block
      context = contexts.get(contextName);

      if (context == null) {
        context = new Context(cconfig);
        contexts.put(contextName, context);
      } else if (!context.cconfig.equals(cconfig)) {
        contextToClose = context;
        context = new Context(cconfig);
        contexts.put(contextName, context);
      }
    }

    if (contextToClose != null)
      contextToClose.close();

    ClassLoader loader = context.getClassLoader();
    if (loader == null) {
      // oops, context was closed by another thread, try again
      return getClassLoader(contextName);
    }

    return loader;

  }

  public <U> Class<? extends U> loadClass(String context, String classname, Class<U> extension) throws ClassNotFoundException {
    try {
      return getClassLoader(context).loadClass(classname).asSubclass(extension);
    } catch (IOException e) {
      throw new ClassNotFoundException("IO Error loading class " + classname, e);
    }
  }

  public void removeUnusedContexts(Set<String> configuredContexts) {

    Map<String,Context> unused;

    // ContextManager knows of some set of contexts. This method will be called with
    // the set of currently configured contexts. We will close the contexts that are
    // no longer in the configuration.
    synchronized (this) {
      unused = new HashMap<>(contexts);
      unused.keySet().removeAll(configuredContexts);
      contexts.keySet().removeAll(unused.keySet());
    }

    for (Entry<String,Context> e : unused.entrySet()) {
      // close outside of lock
      log.info("Closing unused context: {}", e.getKey());
      e.getValue().close();
    }
  }
}
