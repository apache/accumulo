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
package org.apache.accumulo.core.classloader;

import java.lang.reflect.InvocationTargetException;

import org.apache.accumulo.core.spi.common.ClassLoaderFactory;
import org.apache.accumulo.core.spi.common.ClassLoaderFactory.Printer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(AccumuloClassLoader.class);

  private static final String CLASS_LOADER_FACTORY = "general.class.loader.factory";
  private static ClassLoaderFactory FACTORY = null;
  private static ClassLoader CLASSLOADER = null;

  /**
   * Initialize the AccumuloClassLoader
   */
  @SuppressWarnings("unchecked")
  public static synchronized ClassLoader getClassLoader() throws Exception {
    if (null == CLASSLOADER) {
      LOG.info("Creating ClassLoader");
      var factoryName = System.getProperty(CLASS_LOADER_FACTORY, "");
      if (factoryName.isBlank()) {
        LOG.info("No ClassLoader specified, using the system classloader");
        CLASSLOADER = ClassLoader.getSystemClassLoader();
      } else {
        try {
          var factoryClass = Class.forName(factoryName);
          if (ClassLoaderFactory.class.isAssignableFrom(factoryClass)) {
            LOG.info("Creating ClassLoader: {}", factoryName);
            FACTORY = ((Class<? extends ClassLoaderFactory>) factoryClass).getDeclaredConstructor()
                .newInstance();
            CLASSLOADER = FACTORY.getClassLoader();
          } else {
            throw new RuntimeException(factoryName + " does not implement ClassLoaderFactory");
          }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
            | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
            | SecurityException e) {
          LOG.error(
              "Unable to load and initialize class: {}. Ensure that the jar containing the ClassLoaderFactory is on the classpath",
              factoryName);
          throw e;
        }
      }
    }
    return CLASSLOADER;
  }

  public static void printClassPath(Printer out, boolean debug) {
    if (null != FACTORY) {
      FACTORY.printClassPath(CLASSLOADER, out, debug);
    } else {
      out.print("Using standard VM classloader");
    }
  }

  static synchronized <U> Class<? extends U> loadClass(String classname, Class<U> extension)
      throws ClassNotFoundException {
    try {
      return getClassLoader().loadClass(classname).asSubclass(extension);
    } catch (Exception e) {
      throw new ClassNotFoundException("IO Error loading class " + classname, e);
    }
  }

}
