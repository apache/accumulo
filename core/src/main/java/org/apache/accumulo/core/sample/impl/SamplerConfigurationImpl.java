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
package org.apache.accumulo.core.sample.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Writable;

public class SamplerConfigurationImpl implements Writable {
  private String className;
  private Map<String,String> options;

  public SamplerConfigurationImpl(DataInput in) throws IOException {
    readFields(in);
  }

  public SamplerConfigurationImpl(SamplerConfiguration sc) {
    this.className = sc.getSamplerClassName();
    this.options = new HashMap<>(sc.getOptions());
  }

  public SamplerConfigurationImpl(String className, Map<String,String> options) {
    this.className = className;
    this.options = options;
  }

  public SamplerConfigurationImpl() {}

  public String getClassName() {
    return className;
  }

  public Map<String,String> getOptions() {
    return Collections.unmodifiableMap(options);
  }

  @Override
  public int hashCode() {
    return 31 * className.hashCode() + options.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SamplerConfigurationImpl) {
      SamplerConfigurationImpl osc = (SamplerConfigurationImpl) o;

      return className.equals(osc.className) && options.equals(osc.options);
    }

    return false;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // The Writable serialization methods for this class are called by RFile and therefore must be
    // very stable. An alternative way to serialize this class is to
    // use Thrift. That was not used here inorder to avoid making RFile depend on Thrift.

    // versioning info
    out.write(1);

    out.writeUTF(className);

    out.writeInt(options.size());

    for (Entry<String,String> entry : options.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int version = in.readByte();

    if (version != 1) {
      throw new IllegalArgumentException("Unexpected version " + version);
    }

    className = in.readUTF();

    options = new HashMap<>();

    int num = in.readInt();

    for (int i = 0; i < num; i++) {
      String key = in.readUTF();
      String val = in.readUTF();
      options.put(key, val);
    }
  }

  public SamplerConfiguration toSamplerConfiguration() {
    SamplerConfiguration sc = new SamplerConfiguration(className);
    sc.setOptions(options);
    return sc;
  }

  public List<Pair<String,String>> toTableProperties() {
    ArrayList<Pair<String,String>> props = new ArrayList<>();

    for (Entry<String,String> entry : options.entrySet()) {
      props
          .add(new Pair<>(Property.TABLE_SAMPLER_OPTS.getKey() + entry.getKey(), entry.getValue()));
    }

    // intentionally added last, so its set last
    props.add(new Pair<>(Property.TABLE_SAMPLER.getKey(), className));

    return props;
  }

  public Map<String,String> toTablePropertiesMap() {
    LinkedHashMap<String,String> propsMap = new LinkedHashMap<>();
    for (Pair<String,String> pair : toTableProperties()) {
      propsMap.put(pair.getFirst(), pair.getSecond());
    }

    return propsMap;
  }

  public static SamplerConfigurationImpl newSamplerConfig(AccumuloConfiguration acuconf) {
    String className = acuconf.get(Property.TABLE_SAMPLER);

    if (className == null || className.equals("")) {
      return null;
    }

    Map<String,String> rawOptions = acuconf.getAllPropertiesWithPrefix(Property.TABLE_SAMPLER_OPTS);
    Map<String,String> options = new HashMap<>();

    for (Entry<String,String> entry : rawOptions.entrySet()) {
      String key = entry.getKey().substring(Property.TABLE_SAMPLER_OPTS.getKey().length());
      options.put(key, entry.getValue());
    }

    return new SamplerConfigurationImpl(className, options);
  }

  @Override
  public String toString() {
    return className + " " + options;
  }

  public static TSamplerConfiguration toThrift(SamplerConfiguration samplerConfig) {
    if (samplerConfig == null) {
      return null;
    }
    return new TSamplerConfiguration(samplerConfig.getSamplerClassName(),
        samplerConfig.getOptions());
  }

  public static SamplerConfiguration fromThrift(TSamplerConfiguration tsc) {
    if (tsc == null) {
      return null;
    }
    return new SamplerConfiguration(tsc.getClassName()).setOptions(tsc.getOptions());
  }

}
