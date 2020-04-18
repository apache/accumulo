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
package org.apache.accumulo.core.client.admin;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.Map;

/**
 *
 * @since 2.1.0
 */
abstract class PluginConfig<T extends PluginConfig<T>> {

  private String className;
  private Map<String,String> options = Collections.emptyMap();

  /**
   * @param className
   *          The name of a class that implements
   *          org.apache.accumulo.tserver.compaction.CompactionStrategy. This class must be exist on
   *          tservers.
   */
  protected PluginConfig(String className) {
    requireNonNull(className);
    this.className = className;
  }

  /**
   * @return the class name passed to the constructor.
   */
  public String getClassName() {
    return className;
  }

  /**
   * @param opts
   *          The options that will be passed to the init() method of the compaction strategy when
   *          its instantiated on a tserver. This method will copy the map. The default is an empty
   *          map.
   * @return this
   */
  public T setOptions(Map<String,String> opts) {
    requireNonNull(opts);
    this.options = Map.copyOf(opts);
    return (T) this;
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
