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

package org.apache.accumulo.core.client.admin;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;

/**
 * This class encapsultes configuration and options needed to setup and use sampling.
 *
 * @since 1.8.0
 */

public class SamplerConfiguration {

  private String className;
  private Map<String,String> options = new HashMap<>();

  public SamplerConfiguration(String samplerClassName) {
    Preconditions.checkNotNull(samplerClassName);
    this.className = samplerClassName;
  }

  public SamplerConfiguration setOptions(Map<String,String> options) {
    Preconditions.checkNotNull(options);
    this.options = new HashMap<>(options.size());

    for (Entry<String,String> entry : options.entrySet()) {
      addOption(entry.getKey(), entry.getValue());
    }

    return this;
  }

  public SamplerConfiguration addOption(String option, String value) {
    checkArgument(option != null, "option is null");
    checkArgument(value != null, "value is null");
    this.options.put(option, value);
    return this;
  }

  public Map<String,String> getOptions() {
    return Collections.unmodifiableMap(options);
  }

  public String getSamplerClassName() {
    return className;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SamplerConfiguration) {
      SamplerConfiguration osc = (SamplerConfiguration) o;

      return className.equals(osc.className) && options.equals(osc.options);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return className.hashCode() + 31 * options.hashCode();
  }

  @Override
  public String toString() {
    return className + " " + options;
  }
}
