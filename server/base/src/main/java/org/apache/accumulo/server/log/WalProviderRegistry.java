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
package org.apache.accumulo.server.log;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.PropertyType;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.wal.WalProvider;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Loads and validates the configured WAL providers for a server process. */
public class WalProviderRegistry {

  private static final Logger log = LoggerFactory.getLogger(WalProviderRegistry.class);
  private static final ObjectMapper JSON_MAPPER =
      new ObjectMapper().enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  private final Map<String,WalProvider> providers = new HashMap<>();
  private final Map<String,WalProvider> providersByClass = new HashMap<>();
  private final WalProvider current;

  static void validateProviderId(String id, String className) {
    if (id == null || !id.matches(WalProvider.PROVIDER_ID_REGEX)) {
      throw new IllegalArgumentException("Invalid WAL provider ID '" + id + "' from " + className
          + "; IDs must match " + WalProvider.PROVIDER_ID_REGEX);
    }
  }

  public WalProviderRegistry(ServerContext context) {
    var conf = context.getConfiguration();
    ClassLoaderUtil.initContextFactory(conf);
    var environment = new ServiceEnvironmentImpl(context);
    String currentClass = conf.get(Property.INSTANCE_WAL_PROVIDER).trim();
    if (currentClass.isEmpty()) {
      throw new IllegalArgumentException(
          "No current WAL provider configured in " + Property.INSTANCE_WAL_PROVIDER);
    }
    var providerOptions = parseProviderConfig(conf.get(Property.INSTANCE_WAL_PROVIDERS));
    for (var entry : providerOptions.entrySet()) {
      String className = entry.getKey();
      try {
        WalProvider provider =
            ConfigurationTypeHelper.getClassInstance(null, className, WalProvider.class);
        provider.init(new ProviderInitParameters(environment, entry.getValue()));
        String id = provider.getId();
        validateProviderId(id, className);
        if (id.equals(DfsWalProvider.PROVIDER_ID)
            && !className.equals(DfsWalProvider.class.getName())) {
          throw new IllegalArgumentException("WAL provider ID '" + DfsWalProvider.PROVIDER_ID
              + "' is reserved for legacy filesystem WALs and " + DfsWalProvider.class.getName());
        }
        WalProvider previous = providers.putIfAbsent(id, provider);
        if (previous != null) {
          throw new IllegalArgumentException("Duplicate WAL provider ID '" + id + "' from "
              + previous.getClass().getName() + " and " + className);
        }
        providersByClass.put(className, provider);
        log.info("Loaded WAL provider {} with ID {}", className, id);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("Unable to load configured WAL provider " + className, e);
      }
    }

    current = providersByClass.get(currentClass);
    if (current == null) {
      throw new IllegalArgumentException(
          "Current WAL provider " + currentClass + " from " + Property.INSTANCE_WAL_PROVIDER
              + " is not listed in " + Property.INSTANCE_WAL_PROVIDERS);
    }
  }

  static Map<String,Map<String,String>> parseProviderConfig(String json) {
    final JsonNode root;
    try {
      root = JSON_MAPPER.readTree(json);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON in " + Property.INSTANCE_WAL_PROVIDERS, e);
    }
    if (root == null || !root.isObject()) {
      throw new IllegalArgumentException(Property.INSTANCE_WAL_PROVIDERS
          + " must be a JSON object mapping provider classes to option maps");
    }

    Map<String,Map<String,String>> providers = new LinkedHashMap<>();
    root.fields().forEachRemaining(entry -> {
      String className = entry.getKey();
      if (className.isBlank() || !PropertyType.CLASSNAME.isValidFormat(className)) {
        throw new IllegalArgumentException("Invalid WAL provider class name '" + className + "'");
      }
      JsonNode optionsNode = entry.getValue();
      if (!optionsNode.isObject()) {
        throw new IllegalArgumentException("Options for WAL provider " + className
            + " must be a JSON object (use {} when there are no options)");
      }
      Map<String,String> options = new LinkedHashMap<>();
      optionsNode.fields().forEachRemaining(option -> {
        if (!option.getValue().isTextual()) {
          throw new IllegalArgumentException("Option '" + option.getKey() + "' for WAL provider "
              + className + " must have a string value");
        }
        options.put(option.getKey(), option.getValue().textValue());
      });
      providers.put(className, Collections.unmodifiableMap(options));
    });
    return Collections.unmodifiableMap(providers);
  }

  private record ProviderInitParameters(ServiceEnvironment environment, Map<String,String> options)
      implements WalProvider.InitParameters {
    @Override
    public ServiceEnvironment getServiceEnvironment() {
      return environment;
    }

    @Override
    public Map<String,String> getOptions() {
      return options;
    }
  }

  public WalProvider getCurrent() {
    return current;
  }

  public WalProvider get(String id) throws ProviderNotFoundException {
    WalProvider provider = providers.get(id);
    if (provider == null) {
      throw new ProviderNotFoundException(id);
    }
    return provider;
  }

  public Set<String> getIds() {
    return Set.copyOf(providers.keySet());
  }

  public static class ProviderNotFoundException extends IOException {
    private static final long serialVersionUID = 1L;

    public ProviderNotFoundException(String id) {
      super("WAL provider with ID '" + id + "' is not configured in "
          + Property.INSTANCE_WAL_PROVIDERS);
    }
  }
}
