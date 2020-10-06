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

/**
 * The ClassLoaderFactory implementation is defined by the property general.class.loader.factory.
 * The implementation will return a ClassLoader to be used for dynamically loading classes.
 */
public interface ClassLoaderFactory {

  public interface Printer {
    void print(String s);
  }

  /**
   * Return the configured classloader
   *
   * @return classloader the configured classloader
   */
  ClassLoader getClassLoader() throws Exception;

  /**
   * Print the classpath to the Printer
   *
   * @param cl
   *          classloader
   * @param out
   *          printer
   * @param debug
   *          enable debug output
   */
  void printClassPath(ClassLoader cl, Printer out, boolean debug);

}
