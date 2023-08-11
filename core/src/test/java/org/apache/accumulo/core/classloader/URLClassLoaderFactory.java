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
import java.util.stream.Stream;

import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;

// test implementation
public class URLClassLoaderFactory implements ContextClassLoaderFactory {

  private static final String COMMA = ",";

  @Override
  public ClassLoader getClassLoader(String contextName) {
    // The context name is the classpath.
    URL[] urls = Stream.of(contextName.split(COMMA)).map(p -> {
      try {
        return new URL(p);
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException("Error creating URL from classpath segment: " + p, e);
      }
    }).toArray(URL[]::new);
    return URLClassLoader.newInstance(urls);
  }

}
