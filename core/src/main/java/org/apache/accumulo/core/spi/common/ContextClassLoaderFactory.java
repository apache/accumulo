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
package org.apache.accumulo.core.spi.common;

/**
 * The ContextClassLoaderFactory provides a mechanism for various Accumulo components to use a
 * custom ClassLoader for specific contexts, such as loading table iterators. This factory is
 * initialized at startup and supplies ClassLoaders when provided a context.
 *
 * <p>
 * This factory can be configured using the <code>general.context.class.loader.factory</code>
 * property. All implementations of this factory must have a default (no-argument) public
 * constructor.
 *
 * <p>
 * A default implementation is provided for Accumulo 2.x to retain existing context class loader
 * behavior based on per-table configuration. However, after Accumulo 2.x, the default is expected
 * to change to a simpler implementation, and users will need to provide their own implementation to
 * support advanced context class loading features. Some implementations may be maintained by the
 * Accumulo developers in a separate package. Check the Accumulo website or contact the developers
 * for more details on the status of these implementations.
 *
 * <p>
 * Because this factory is expected to be instantiated early in the application startup process,
 * configuration is expected to be provided within the environment (such as in Java system
 * properties or process environment variables), and is implementation-specific.
 *
 * @since 2.1.0
 */
public interface ContextClassLoaderFactory {

  /**
   * Get the class loader for the given contextName. Callers should not cache the ClassLoader result
   * as it may change if/when the ClassLoader reloads. Implementations should throw a
   * RuntimeException of some type (such as IllegalArgumentException) if the provided contextName is
   * not supported or fails to be constructed.
   *
   * @param contextName the name of the context that represents a class loader that is managed by
   *        this factory (can be null)
   * @return the class loader for the given contextName
   */
  ClassLoader getClassLoader(String contextName);

}
