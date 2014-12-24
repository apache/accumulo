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

import java.io.File;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Encapsulate calls to PropertyConfigurator or DOMConfigurator to set up logging
 */
public class Log4jConfiguration {

  private final boolean usingProperties;
  private final String filename;
  private final File log4jFile;
  private final String auditConfig;

  public Log4jConfiguration(String filename) {
    usingProperties = (filename != null && filename.endsWith(".properties"));
    this.filename = filename;
    log4jFile = new File(filename);

    // Read the auditing config
    auditConfig = String.format("%s/auditLog.xml", System.getenv("ACCUMULO_CONF_DIR"));
  }

  public boolean isUsingProperties() {
    return usingProperties;
  }

  public void resetLogger() {
    // Force a reset on the logger's configuration, but only if the configured log4j file actually exists
    // If we reset the configuration blindly, the ITs will not get any logging as they don't set it up on their own
    if (log4jFile.exists() && log4jFile.isFile() && log4jFile.canRead()) {
      LogManager.resetConfiguration();
      if (usingProperties) {
        new PropertyConfigurator().doConfigure(filename, LogManager.getLoggerRepository());
      } else {
        new DOMConfigurator().doConfigure(filename, LogManager.getLoggerRepository());
      }

      // Watch the auditLog.xml for the future updates. Because we reset the subsystem, we have to reconfigure auditing, too.
      DOMConfigurator.configureAndWatch(auditConfig, 5000l);
    }

  }
}
