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
package org.apache.accumulo.core.spi.common;

import org.apache.accumulo.core.client.PluginEnvironment.Configuration;

/**
 * The ClassLoaderFactory is defined by the property general.context.factory. The factory
 * implementation is configured externally to Accumulo and will return a ClassLoader for a given
 * contextName.
 *
 * @since 2.1.0
 *
 */
public interface ContextClassLoaderFactory {

  /**
   * Initialize the ClassLoaderFactory. Implementations may need a reference to the configuration so
   * that it can clean up contexts that are no longer being used.
   *
   * @param contextProperties
   *          Accumulo configuration properties
   * @throws Exception
   *           if error initializing ClassLoaderFactory
   */
  void initialize(Configuration contextProperties) throws Exception;

  /**
   * Get the classloader for the context name. Callers should not cache the ClassLoader result as it
   * may change if/when the ClassLoader reloads
   *
   * @param contextName
   *          name of classloader context
   * @return classloader configured for the context
   * @throws IllegalArgumentException
   *           if contextName is not supported
   */
  ClassLoader getClassLoader(String contextName) throws IllegalArgumentException;

}
