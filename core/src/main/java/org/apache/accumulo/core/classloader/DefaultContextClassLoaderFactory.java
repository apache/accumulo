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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * The default implementation of ContextClassLoaderFactory. This implementation is subject to change
 * over time. It currently implements the legacy context class loading behavior based on Accumulo's
 * custom class loaders and commons-vfs2. In future, it may simply return the system class loader
 * for all requested contexts. This class is used internally to Accumulo only, and should not be
 * used by users in their configuration.
 */
public class DefaultContextClassLoaderFactory implements ContextClassLoaderFactory {

  private static final AtomicBoolean isInstantiated = new AtomicBoolean(false);
  private static final Logger LOG = LoggerFactory.getLogger(DefaultContextClassLoaderFactory.class);
  private static final String className = DefaultContextClassLoaderFactory.class.getName();

  // Do we set a max size here and how long until we expire the classloader?
  private final Cache<String,Context> contexts =
      Caffeine.newBuilder().maximumSize(100).expireAfterAccess(1, TimeUnit.DAYS).build();

  public DefaultContextClassLoaderFactory() {
    if (!isInstantiated.compareAndSet(false, true)) {
      throw new IllegalStateException("Can only instantiate " + className + " once");
    }
  }

  @Override
  public ClassLoader getClassLoader(String contextName) {
    if (contextName == null) {
      throw new IllegalArgumentException("Unknown context");
    }

    final ClassLoader loader = contexts.get(contextName, Context::new).getClassLoader();

    LOG.debug("Returning classloader {} for context {}", loader.getClass().getName(), contextName);
    return loader;
  }

  private static class Context {
    private final String contextName;
    private ClassLoader loader;

    Context(String contextName) {
      this.contextName = contextName;
    }

    synchronized ClassLoader getClassLoader() {
      if (loader == null) {
        LOG.debug("ClassLoader not created for context, creating new one. uris: {}", contextName);
        loader = new URLClassLoader(Arrays.stream(contextName.split(",")).map(url -> {
          try {
            return new URL(url);
          } catch (MalformedURLException e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toList()).toArray(new URL[] {}), ClassLoader.getSystemClassLoader());
      }
      return loader;
    }
  }

}
