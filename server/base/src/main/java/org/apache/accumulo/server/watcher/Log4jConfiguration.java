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
package org.apache.accumulo.server.watcher;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Encapsulate calls to PropertyConfigurator or DOMConfigurator to set up logging
 */
public class Log4jConfiguration {

  private final boolean usingProperties;
  private final String filename;

  public Log4jConfiguration(String filename) {
    usingProperties = (filename != null && filename.endsWith(".properties"));
    this.filename = filename;
  }

  public boolean isUsingProperties() {
    return usingProperties;
  }

  public void resetLogger() {
    // Force a reset on the logger's configuration
    LogManager.resetConfiguration();
    if (usingProperties) {
      new PropertyConfigurator().doConfigure(filename, LogManager.getLoggerRepository());
    } else {
      new DOMConfigurator().doConfigure(filename, LogManager.getLoggerRepository());
    }
  }
}
