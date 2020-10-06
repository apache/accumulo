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

import java.lang.reflect.Method;

import org.apache.accumulo.core.spi.common.ClassLoaderFactory;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated(since = "2.1.0", forRemoval = true)
public class LegacyVFSClassLoaderFactory implements ClassLoaderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(LegacyVFSClassLoaderFactory.class);

  @Override
  public ClassLoader getClassLoader() throws Exception {
    ClassLoader generalClassPathsLoader =
        org.apache.accumulo.start.classloader.AccumuloClassLoader.getClassLoader();
    Class<?> legacyVFSClassLoaderClass = generalClassPathsLoader
        .loadClass("org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader");
    ClassLoader cl =
        (ClassLoader) legacyVFSClassLoaderClass.getMethod("getClassLoader").invoke(null);
    // Preload classes that cause a deadlock between the ServiceLoader and the DFSClient when
    // using the VFSClassLoader with jars in HDFS.
    Class<?> confClass = null;
    try {
      confClass =
          AccumuloClassLoader.getClassLoader().loadClass("org.apache.hadoop.conf.Configuration");
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to find Hadoop Configuration class on classpath, check configuration.", e);
      throw e;
    }
    Object conf = null;
    try {
      conf = confClass.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.error("Error creating new instance of Hadoop Configuration", e);
      throw e;
    }
    try {
      Method getClassByNameOrNullMethod =
          conf.getClass().getMethod("getClassByNameOrNull", String.class);
      getClassByNameOrNullMethod.invoke(conf, "org.apache.hadoop.mapred.JobConf");
      getClassByNameOrNullMethod.invoke(conf, "org.apache.hadoop.mapred.JobConfigurable");
    } catch (Exception e) {
      LOG.error("Error pre-loading JobConf and JobConfigurable classes, VFS classloader with "
          + "system classes in HDFS may not work correctly", e);
      throw e;
    }
    return cl;
  }

  @Override
  public void printClassPath(ClassLoader cl, Printer out, boolean debug) {
    AccumuloVFSClassLoader.printClassPath(s -> {
      out.print(s);
    }, debug);
  }

}
