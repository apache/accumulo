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
package org.apache.accumulo.core.client.admin;

import static java.util.Objects.requireNonNull;

import java.util.Map;

/**
 * Encapsulates the configuration of an Accumulo server side plugin, which consist of a class name
 * and options.
 *
 * @since 2.1.0
 */
public class PluginConfig {

  private final String className;
  private final Map<String,String> options;

  /**
   * @param className The name of a class that implements a server side plugin. This class must
   *        exist on the server side classpath.
   */
  public PluginConfig(String className) {
    this.className = requireNonNull(className);
    this.options = Map.of();
  }

  /**
   *
   * @param className The name of a class that implements a server side plugin. This class must
   *        exist on the server side classpath.
   * @param options The options that will be passed to the init() method of the plugin when its
   *        instantiated server side. This method will copy the map. The default is an empty map.
   */
  public PluginConfig(String className, Map<String,String> options) {
    this.className = requireNonNull(className);
    this.options = Map.copyOf(options);
  }

  /**
   * @return the class name passed to the constructor.
   */
  public String getClassName() {
    return className;
  }

  /**
   * @return The previously set options. Returns an unmodifiable map. The default is an empty map.
   */
  public Map<String,String> getOptions() {
    return options;
  }

  @Override
  public int hashCode() {
    return className.hashCode() + options.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PluginConfig) {
      PluginConfig ocsc = (PluginConfig) o;
      return className.equals(ocsc.className) && options.equals(ocsc.options);
    }

    return false;
  }

  @Override
  public String toString() {
    return "[className=" + className + ", options=" + options + "]";
  }
}
