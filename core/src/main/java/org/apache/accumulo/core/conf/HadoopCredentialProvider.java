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
package org.apache.accumulo.core.conf;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shim around Hadoop's CredentialProviderFactory provided by hadoop-common.
 */
public class HadoopCredentialProvider {
  private static final Logger log = LoggerFactory.getLogger(HadoopCredentialProvider.class);

  private static final String CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";

  // access to cachedProviders should be synchronized when necessary
  private static final ConcurrentHashMap<String,List<CredentialProvider>> cachedProviders =
      new ConcurrentHashMap<>();

  /**
   * Set the Hadoop Credential Provider path in the provided Hadoop Configuration.
   *
   * @param conf the Hadoop Configuration object
   * @param path the credential provider paths to set
   */
  public static void setPath(Configuration conf, String path) {
    conf.set(CREDENTIAL_PROVIDER_PATH, path);
  }

  /**
   * Fetch/cache the configured providers.
   *
   * @return The List of CredentialProviders, or null if they could not be loaded
   */
  private static List<CredentialProvider> getProviders(Configuration conf) {
    String path = conf.get(CREDENTIAL_PROVIDER_PATH);
    if (path == null || path.isEmpty()) {
      log.debug("Failed to get CredentialProviders; no provider path specified");
      return null;
    }
    final List<CredentialProvider> providers;
    try {
      providers = CredentialProviderFactory.getProviders(conf);
    } catch (IOException e) {
      log.warn("Exception invoking CredentialProviderFactory.getProviders(conf)", e);
      return null;
    }
    return cachedProviders.computeIfAbsent(path, p -> providers);
  }

  /**
   * Attempt to extract the password from any configured CredentialProviders for the given alias. If
   * no providers or credential is found, null is returned.
   *
   * @param conf Configuration for CredentialProvider
   * @param alias Name of CredentialEntry key
   * @return The credential if found, null otherwise
   */
  public static char[] getValue(Configuration conf, String alias) {
    requireNonNull(alias);
    List<CredentialProvider> providerList = getProviders(requireNonNull(conf));
    return providerList == null ? null : providerList.stream().map(provider -> {
      try {
        return provider.getCredentialEntry(alias);
      } catch (IOException e) {
        log.warn("Failed to call getCredentialEntry(alias) for provider {}", provider, e);
        return null;
      }
    }).filter(Objects::nonNull).map(CredentialProvider.CredentialEntry::getCredential).findFirst()
        .orElseGet(() -> {
          // If we didn't find it, this isn't an error, it just wasn't set in the CredentialProvider
          log.trace("Could not extract credential for {} from providers", alias);
          return null;
        });
  }

  /**
   * Attempt to extract all aliases from any configured CredentialProviders.
   *
   * @param conf Configuration for the CredentialProvider
   * @return A list of aliases. An empty list if no CredentialProviders are configured, or the
   *         providers are empty.
   */
  public static List<String> getKeys(Configuration conf) {
    List<CredentialProvider> providerList = getProviders(requireNonNull(conf));
    return providerList == null ? Collections.emptyList()
        : providerList.stream().flatMap(provider -> {
          List<String> aliases = null;
          try {
            aliases = provider.getAliases();
          } catch (IOException e) {
            log.warn("Problem getting aliases from provider {}", provider, e);
          }
          return aliases == null ? Stream.empty() : aliases.stream();
        }).collect(Collectors.toList());
  }

  /**
   * Create a CredentialEntry using the configured Providers. If multiple CredentialProviders are
   * configured, the first will be used.
   *
   * @param conf Configuration for the CredentialProvider
   * @param name CredentialEntry name (alias)
   * @param credential The credential
   */
  public static void createEntry(Configuration conf, String name, char[] credential)
      throws IOException {
    requireNonNull(conf);
    requireNonNull(name);
    requireNonNull(credential);

    List<CredentialProvider> providers = getProviders(conf);
    if (providers == null || providers.isEmpty()) {
      throw new IOException("Could not fetch any CredentialProviders");
    }

    CredentialProvider provider = providers.get(0);
    if (providers.size() != 1) {
      log.warn("Found more than one CredentialProvider. Using first provider found ({})", provider);
    }
    provider.createCredentialEntry(name, credential);
    provider.flush();
  }

}
