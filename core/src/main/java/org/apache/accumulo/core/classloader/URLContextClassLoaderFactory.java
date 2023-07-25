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
package org.apache.accumulo.core.classloader;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;

/**
 * The default implementation of ContextClassLoaderFactory. This classloader returns a
 * URLClassLoader based on the given context value which is a CSV list of URLs. For example,
 * file://path/one/jar1.jar,file://path/two/jar2.jar
 */
public class URLContextClassLoaderFactory implements ContextClassLoaderFactory {

  private static final AtomicBoolean isInstantiated = new AtomicBoolean(false);
  private static final Logger LOG = LoggerFactory.getLogger(URLContextClassLoaderFactory.class);
  private static final String className = URLContextClassLoaderFactory.class.getName();

  // Cache the class loaders for re-use
  // WeakReferences are used so that the class loaders can be cleaned up when no longer needed
  // Classes that are loaded contain a reference to the class loader used to load them
  // so the class loader will be garbage collected when no more classes are loaded that reference it
  private final Cache<String,URLClassLoader> classloaders =
      Caches.getInstance().createNewBuilder(CacheName.CLASSLOADERS, true).weakValues().build();

  public URLContextClassLoaderFactory() {
    if (!isInstantiated.compareAndSet(false, true)) {
      throw new IllegalStateException("Can only instantiate " + className + " once");
    }
  }

  @Override
  public ClassLoader getClassLoader(String context) {
    if (context == null) {
      throw new IllegalArgumentException("Unknown context");
    }

    return classloaders.get(context, k -> {
      LOG.debug("Creating URLClassLoader for context, uris: {}", context);
      return new URLClassLoader(Arrays.stream(context.split(",")).map(url -> {
        try {
          return new URL(url);
        } catch (MalformedURLException e) {
          throw new RuntimeException(e);
        }
      }).toArray(URL[]::new), ClassLoader.getSystemClassLoader());
    });
  }
}
