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
package org.apache.accumulo.cluster;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Property-based configuration of options to control how SSH is performed.
 *
 * A properties file can be provided using {@link #SSH_PROPERTIES_FILE} or using the normal system properties. Any relevant properties set in the system
 * properties take precedence over the provided file.
 */
public class RemoteShellOptions {
  private static final Logger log = LoggerFactory.getLogger(RemoteShellOptions.class);

  public static final String SSH_PREFIX = "accumulo.cluster.ssh.";

  public static final String SSH_COMMAND_KEY = SSH_PREFIX + "command";
  public static final String SSH_COMMAND_DEFAULT = "/usr/bin/ssh";

  public static final String SSH_OPTIONS_KEY = SSH_PREFIX + "options";
  public static final String SSH_OPTIONS_DEFAULT = "";

  public static final String SSH_USER_KEY = SSH_PREFIX + "user";
  public static final String SSH_USER_DEFAULT = "";

  public static final String SSH_PROPERTIES_FILE = "accumulo.ssh.properties";

  protected Properties properties = new Properties();

  public RemoteShellOptions() {
    properties = new Properties();
    Properties systemProperties = System.getProperties();

    String propertyFile = systemProperties.getProperty(SSH_PROPERTIES_FILE);

    // Load properties from the specified file
    if (null != propertyFile) {
      // Check for properties provided in a file
      File f = new File(propertyFile);
      if (f.exists() && f.isFile() && f.canRead()) {
        FileReader reader = null;
        try {
          reader = new FileReader(f);
        } catch (FileNotFoundException e) {
          log.warn("Could not read properties from specified file: {}", propertyFile, e);
        }

        if (null != reader) {
          try {
            properties.load(reader);
          } catch (IOException e) {
            log.warn("Could not load properties from specified file: {}", propertyFile, e);
          } finally {
            try {
              reader.close();
            } catch (IOException e) {
              log.warn("Could not close reader", e);
            }
          }
        }
      }
    }

    // Let other system properties override those in the file
    for (Entry<Object,Object> entry : systemProperties.entrySet()) {
      if (!(entry.getKey() instanceof String)) {
        continue;
      }

      String key = (String) entry.getKey();
      if (key.startsWith(SSH_PREFIX)) {
        properties.put(key, entry.getValue());
      }
    }
  }

  /**
   * Fetch the configured user to ssh as
   *
   * @return The user to SSH as, default {@link #SSH_USER_DEFAULT}
   */
  public String getUserName() {
    return properties.getProperty(SSH_USER_KEY, SSH_USER_DEFAULT);
  }

  /**
   * Extra SSH options that can be provided
   *
   * @return Any SSH options, default {@link #SSH_OPTIONS_DEFAULT}
   */
  public String getSshOptions() {
    return properties.getProperty(SSH_OPTIONS_KEY, SSH_OPTIONS_DEFAULT);
  }

  /**
   * The explicit SSH executable to invoke
   *
   * @return The configured SSH executable, default {@link #SSH_COMMAND_DEFAULT}
   */
  public String getSshCommand() {
    return properties.getProperty(SSH_COMMAND_KEY, SSH_COMMAND_DEFAULT);
  }
}
