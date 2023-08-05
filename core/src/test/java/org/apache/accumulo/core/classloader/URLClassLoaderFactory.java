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

import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// test implementation
public class URLClassLoaderFactory implements ContextClassLoaderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(URLClassLoaderFactory.class);
  private static final String COMMA = ",";

  private URL[] parseURLsFromContextName(String contextName) throws MalformedURLException {
    String[] parts = contextName.split(COMMA);
    URL[] urls = new URL[parts.length];
    for (int i = 0; i < parts.length; i++) {
      urls[i] = new URL(parts[i]);
    }
    return urls;
  }

  @Override
  public ClassLoader getClassLoader(String contextName) {
    // The context name is the classpath.
    try {
      URL[] urls = parseURLsFromContextName(contextName);
      return URLClassLoader.newInstance(urls);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Error creating URL from contextName: " + contextName, e);
    }
  }

  @Override
  public boolean isValid(String contextName) {
    try {
      parseURLsFromContextName(contextName);
      return true;
    } catch (MalformedURLException e) {
      LOG.error("Error creating URL from contextName: " + contextName, e);
      return false;
    }
  }

}
