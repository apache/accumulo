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
package org.apache.accumulo.server;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

import com.google.common.collect.ImmutableMap;

public class ServiceEnvironmentImpl implements ServiceEnvironment {

  private final ServerContext srvCtx;
  private final Configuration conf;

  public ServiceEnvironmentImpl(ServerContext ctx) {
    this.srvCtx = ctx;
    this.conf = new ConfigurationImpl(srvCtx.getConfiguration());
  }

  private static class ConfigurationImpl implements Configuration {

    private final AccumuloConfiguration acfg;
    private Map<String,String> customProps;
    private Map<String,String> tableCustomProps;

    ConfigurationImpl(AccumuloConfiguration acfg) {
      this.acfg = acfg;
    }

    @Override
    public String get(String key) {
      // Get prop to check if sensitive, also looking up by prop may be more efficient.
      Property prop = Property.getPropertyByKey(key);
      if (prop != null) {
        if (prop.isSensitive()) {
          return null;
        }
        return acfg.get(prop);
      } else {
        return acfg.get(key);
      }
    }

    @Override
    public Map<String,String> getCustom() {
      if (customProps == null) {
        customProps = buildCustom(Property.GENERAL_ARBITRARY_PROP_PREFIX);
      }

      return customProps;
    }

    @Override
    public String getCustom(String keySuffix) {
      return getCustom().get(keySuffix);
    }

    @Override
    public Map<String,String> getTableCustom() {
      if (tableCustomProps == null) {
        tableCustomProps = buildCustom(Property.TABLE_ARBITRARY_PROP_PREFIX);
      }

      return tableCustomProps;
    }

    @Override
    public String getTableCustom(String keySuffix) {
      return getTableCustom().get(keySuffix);
    }

    private Map<String,String> buildCustom(Property customPrefix) {
      // This could be optimized as described in #947
      Map<String,String> props = acfg.getAllPropertiesWithPrefix(customPrefix);
      var builder = ImmutableMap.<String,String>builder();
      props.forEach((k, v) -> {
        builder.put(k.substring(customPrefix.getKey().length()), v);
      });

      return builder.build();
    }

    @Override
    public Iterator<Entry<String,String>> iterator() {
      return StreamSupport.stream(acfg.spliterator(), false)
          .filter(e -> !Property.isSensitive(e.getKey())).iterator();
    }
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public Configuration getConfiguration(TableId tableId) {
    return new ConfigurationImpl(srvCtx.getTableConfiguration(tableId));
  }

  @Override
  public String getTableName(TableId tableId) throws TableNotFoundException {
    return Tables.getTableName(srvCtx, tableId);
  }

  @Override
  public <T> T instantiate(String className, Class<T> base)
      throws ReflectiveOperationException, IOException {
    return ConfigurationTypeHelper.getClassInstance(null, className, base);
  }

  @Override
  public <T> T instantiate(TableId tableId, String className, Class<T> base)
      throws ReflectiveOperationException, IOException {
    String ctx = srvCtx.getTableConfiguration(tableId).get(Property.TABLE_CLASSPATH);
    return ConfigurationTypeHelper.getClassInstance(ctx, className, base);
  }
}
