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
package org.apache.accumulo.core.conf;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Shim around Hadoop: tries to use the CredentialProviderFactory provided by hadoop-common, falling back to a copy inside accumulo-core.
 * <p>
 * The CredentialProvider classes only exist in 2.6.0, so, to use them, we have to do a bunch of reflection. This will also help us to continue to support
 * [2.2.0,2.6.0) when 2.6.0 is officially released.
 */
public class CredentialProviderFactoryShim {
  private static final Logger log = LoggerFactory.getLogger(CredentialProviderFactoryShim.class);

  public static final String HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME = "org.apache.hadoop.security.alias.JavaKeyStoreProvider$Factory";
  public static final String HADOOP_CRED_PROVIDER_FACTORY_GET_PROVIDERS_METHOD_NAME = "getProviders";

  public static final String HADOOP_CRED_PROVIDER_CLASS_NAME = "org.apache.hadoop.security.alias.CredentialProvider";
  public static final String HADOOP_CRED_PROVIDER_GET_CREDENTIAL_ENTRY_METHOD_NAME = "getCredentialEntry";
  public static final String HADOOP_CRED_PROVIDER_GET_ALIASES_METHOD_NAME = "getAliases";
  public static final String HADOOP_CRED_PROVIDER_CREATE_CREDENTIAL_ENTRY_METHOD_NAME = "createCredentialEntry";
  public static final String HADOOP_CRED_PROVIDER_FLUSH_METHOD_NAME = "flush";

  public static final String HADOOP_CRED_ENTRY_CLASS_NAME = "org.apache.hadoop.security.alias.CredentialProvider$CredentialEntry";
  public static final String HADOOP_CRED_ENTRY_GET_CREDENTIAL_METHOD_NAME = "getCredential";

  public static final String CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";

  private static Object hadoopCredProviderFactory = null;
  private static Method getProvidersMethod = null;
  private static Method getAliasesMethod = null;
  private static Method getCredentialEntryMethod = null;
  private static Method getCredentialMethod = null;
  private static Method createCredentialEntryMethod = null;
  private static Method flushMethod = null;
  private static Boolean hadoopClassesAvailable = null;

  /**
   * Determine if we can load the necessary CredentialProvider classes. Only loaded the first time, so subsequent invocations of this method should return fast.
   *
   * @return True if the CredentialProvider classes/methods are available, false otherwise.
   */
  public static synchronized boolean isHadoopCredentialProviderAvailable() {
    // If we already found the class
    if (null != hadoopClassesAvailable) {
      // Make sure everything is initialized as expected
      if (hadoopClassesAvailable && null != getProvidersMethod && null != hadoopCredProviderFactory && null != getCredentialEntryMethod
          && null != getCredentialMethod) {
        return true;
      } else {
        // Otherwise we failed to load it
        return false;
      }
    }

    hadoopClassesAvailable = false;

    // Load Hadoop CredentialProviderFactory
    Class<?> hadoopCredProviderFactoryClz = null;
    try {
      hadoopCredProviderFactoryClz = Class.forName(HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME);
    } catch (ClassNotFoundException e) {
      log.trace("Could not load class {}", HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME, e);
      return false;
    }

    // Load Hadoop CredentialProviderFactory.getProviders(Configuration)
    try {
      getProvidersMethod = hadoopCredProviderFactoryClz.getMethod(HADOOP_CRED_PROVIDER_FACTORY_GET_PROVIDERS_METHOD_NAME, Configuration.class);
    } catch (SecurityException e) {
      log.trace("Could not find {} method on {}", HADOOP_CRED_PROVIDER_FACTORY_GET_PROVIDERS_METHOD_NAME, HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME, e);
      return false;
    } catch (NoSuchMethodException e) {
      log.trace("Could not find {} method on {}", HADOOP_CRED_PROVIDER_FACTORY_GET_PROVIDERS_METHOD_NAME, HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME, e);
      return false;
    }

    // Instantiate Hadoop CredentialProviderFactory
    try {
      hadoopCredProviderFactory = hadoopCredProviderFactoryClz.newInstance();
    } catch (InstantiationException e) {
      log.trace("Could not instantiate class {}", HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME, e);
      return false;
    } catch (IllegalAccessException e) {
      log.trace("Could not instantiate class {}", HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME, e);
      return false;
    }

    // Load Hadoop CredentialProvider
    Class<?> hadoopCredProviderClz = null;
    try {
      hadoopCredProviderClz = Class.forName(HADOOP_CRED_PROVIDER_CLASS_NAME);
    } catch (ClassNotFoundException e) {
      log.trace("Could not load class {}", HADOOP_CRED_PROVIDER_CLASS_NAME, e);
      return false;
    }

    // Load Hadoop CredentialProvider.getCredentialEntry(String)
    try {
      getCredentialEntryMethod = hadoopCredProviderClz.getMethod(HADOOP_CRED_PROVIDER_GET_CREDENTIAL_ENTRY_METHOD_NAME, String.class);
    } catch (SecurityException e) {
      log.trace("Could not find {} method on {}", HADOOP_CRED_PROVIDER_GET_CREDENTIAL_ENTRY_METHOD_NAME, HADOOP_CRED_PROVIDER_CLASS_NAME, e);
      return false;
    } catch (NoSuchMethodException e) {
      log.trace("Could not find {} method on {}", HADOOP_CRED_PROVIDER_GET_CREDENTIAL_ENTRY_METHOD_NAME, HADOOP_CRED_PROVIDER_CLASS_NAME, e);
      return false;
    }

    // Load Hadoop CredentialProvider.getAliases()
    try {
      getAliasesMethod = hadoopCredProviderClz.getMethod(HADOOP_CRED_PROVIDER_GET_ALIASES_METHOD_NAME);
    } catch (SecurityException e) {
      log.trace("Could not find {} method on {}", HADOOP_CRED_PROVIDER_GET_ALIASES_METHOD_NAME, HADOOP_CRED_PROVIDER_CLASS_NAME, e);
      return false;
    } catch (NoSuchMethodException e) {
      log.trace("Could not find {} method on {}", HADOOP_CRED_PROVIDER_GET_ALIASES_METHOD_NAME, HADOOP_CRED_PROVIDER_CLASS_NAME, e);
      return false;
    }

    // Load Hadoop CredentialProvider.createCredentialEntry(String, char[])
    try {
      createCredentialEntryMethod = hadoopCredProviderClz.getMethod(HADOOP_CRED_PROVIDER_CREATE_CREDENTIAL_ENTRY_METHOD_NAME, String.class, char[].class);
    } catch (SecurityException e) {
      log.trace("Could not find {} method on {}", HADOOP_CRED_PROVIDER_CREATE_CREDENTIAL_ENTRY_METHOD_NAME, HADOOP_CRED_PROVIDER_CLASS_NAME, e);
      return false;
    } catch (NoSuchMethodException e) {
      log.trace("Could not find {} method on {}", HADOOP_CRED_PROVIDER_CREATE_CREDENTIAL_ENTRY_METHOD_NAME, HADOOP_CRED_PROVIDER_CLASS_NAME, e);
      return false;
    }

    // Load Hadoop CredentialProvider.flush()
    try {
      flushMethod = hadoopCredProviderClz.getMethod(HADOOP_CRED_PROVIDER_FLUSH_METHOD_NAME);
    } catch (SecurityException e) {
      log.trace("Could not find {} method on {}", HADOOP_CRED_PROVIDER_FLUSH_METHOD_NAME, HADOOP_CRED_PROVIDER_CLASS_NAME, e);
      return false;
    } catch (NoSuchMethodException e) {
      log.trace("Could not find {} method on {}", HADOOP_CRED_PROVIDER_FLUSH_METHOD_NAME, HADOOP_CRED_PROVIDER_CLASS_NAME, e);
      return false;
    }

    // Load Hadoop CredentialEntry
    Class<?> hadoopCredentialEntryClz = null;
    try {
      hadoopCredentialEntryClz = Class.forName(HADOOP_CRED_ENTRY_CLASS_NAME);
    } catch (ClassNotFoundException e) {
      log.trace("Could not load class {}", HADOOP_CRED_ENTRY_CLASS_NAME);
      return false;
    }

    // Load Hadoop CredentialEntry.getCredential()
    try {
      getCredentialMethod = hadoopCredentialEntryClz.getMethod(HADOOP_CRED_ENTRY_GET_CREDENTIAL_METHOD_NAME);
    } catch (SecurityException e) {
      log.trace("Could not find {} method on {}", HADOOP_CRED_ENTRY_GET_CREDENTIAL_METHOD_NAME, HADOOP_CRED_ENTRY_CLASS_NAME, e);
      return false;
    } catch (NoSuchMethodException e) {
      log.trace("Could not find {} method on {}", HADOOP_CRED_ENTRY_GET_CREDENTIAL_METHOD_NAME, HADOOP_CRED_ENTRY_CLASS_NAME, e);
      return false;
    }

    hadoopClassesAvailable = true;

    return true;
  }

  /**
   * Wrapper to fetch the configured {@code List<CredentialProvider>}s.
   *
   * @param conf
   *          Configuration with Property#GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS defined
   * @return The List of CredentialProviders, or null if they could not be loaded
   */
  @SuppressWarnings("unchecked")
  protected static List<Object> getCredentialProviders(Configuration conf) {
    // Call CredentialProviderFactory.getProviders(Configuration)
    Object providersObj = null;
    try {
      providersObj = getProvidersMethod.invoke(hadoopCredProviderFactory, conf);
    } catch (IllegalArgumentException e) {
      log.warn("Could not invoke {}.{}", HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME, HADOOP_CRED_PROVIDER_FACTORY_GET_PROVIDERS_METHOD_NAME, e);
      return null;
    } catch (IllegalAccessException e) {
      log.warn("Could not invoke {}.{}", HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME, HADOOP_CRED_PROVIDER_FACTORY_GET_PROVIDERS_METHOD_NAME, e);
      return null;
    } catch (InvocationTargetException e) {
      log.warn("Could not invoke {}.{}", HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME, HADOOP_CRED_PROVIDER_FACTORY_GET_PROVIDERS_METHOD_NAME, e);
      return null;
    }

    // Cast the Object to List<Object> (actually List<CredentialProvider>)
    try {
      return (List<Object>) providersObj;
    } catch (ClassCastException e) {
      log.error("Expected a List from {} method", HADOOP_CRED_PROVIDER_FACTORY_GET_PROVIDERS_METHOD_NAME, e);
      return null;
    }
  }

  protected static char[] getFromHadoopCredentialProvider(Configuration conf, String alias) {
    List<Object> providerObjList = getCredentialProviders(conf);

    if (null == providerObjList) {
      return null;
    }

    for (Object providerObj : providerObjList) {
      try {
        // Invoke CredentialProvider.getCredentialEntry(String)
        Object credEntryObj = getCredentialEntryMethod.invoke(providerObj, alias);

        if (null == credEntryObj) {
          continue;
        }

        // Then, CredentialEntry.getCredential()
        Object credential = getCredentialMethod.invoke(credEntryObj);

        return (char[]) credential;
      } catch (IllegalArgumentException e) {
        log.warn("Failed to get credential for {} from {}", alias, providerObj, e);
        continue;
      } catch (IllegalAccessException e) {
        log.warn("Failed to get credential for {} from {}", alias, providerObj, e);
        continue;
      } catch (InvocationTargetException e) {
        log.warn("Failed to get credential for {} from {}", alias, providerObj, e);
        continue;
      }
    }

    // If we didn't find it, this isn't an error, it just wasn't set in the CredentialProvider
    log.trace("Could not extract credential for {} from providers", alias);

    return null;
  }

  @SuppressWarnings("unchecked")
  protected static List<String> getAliasesFromHadoopCredentialProvider(Configuration conf) {
    List<Object> providerObjList = getCredentialProviders(conf);

    if (null == providerObjList) {
      log.debug("Failed to get CredProviders");
      return Collections.emptyList();
    }

    ArrayList<String> aliases = new ArrayList<String>();
    for (Object providerObj : providerObjList) {
      if (null != providerObj) {
        Object aliasesObj;
        try {
          aliasesObj = getAliasesMethod.invoke(providerObj);

          if (null != aliasesObj && aliasesObj instanceof List) {
            try {
              aliases.addAll((List<String>) aliasesObj);
            } catch (ClassCastException e) {
              log.warn("Could not cast aliases ({}) from {} to a List<String>", aliasesObj, providerObj, e);
              continue;
            }
          }

        } catch (IllegalArgumentException e) {
          log.warn("Failed to invoke {} on {}", HADOOP_CRED_PROVIDER_GET_ALIASES_METHOD_NAME, providerObj, e);
          continue;
        } catch (IllegalAccessException e) {
          log.warn("Failed to invoke {} on {}", HADOOP_CRED_PROVIDER_GET_ALIASES_METHOD_NAME, providerObj, e);
          continue;
        } catch (InvocationTargetException e) {
          log.warn("Failed to invoke {} on {}", HADOOP_CRED_PROVIDER_GET_ALIASES_METHOD_NAME, providerObj, e);
          continue;
        }
      }
    }

    return aliases;
  }

  /**
   * Create a Hadoop {@link Configuration} with the appropriate members to access CredentialProviders
   *
   * @param credentialProviders
   *          Comma-separated list of CredentialProvider URLs
   * @return Configuration to be used for CredentialProvider
   */
  public static Configuration getConfiguration(String credentialProviders) {
    Preconditions.checkNotNull(credentialProviders);
    return getConfiguration(new Configuration(CachedConfiguration.getInstance()), credentialProviders);
  }

  /**
   * Adds the Credential Provider configuration elements to the provided {@link Configuration}.
   *
   * @param conf
   *          Existing Hadoop Configuration
   * @param credentialProviders
   *          Comma-separated list of CredentialProvider URLs
   */
  public static Configuration getConfiguration(Configuration conf, String credentialProviders) {
    Preconditions.checkNotNull(conf);
    Preconditions.checkNotNull(credentialProviders);
    conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, credentialProviders);
    return conf;
  }

  /**
   * Attempt to extract the password from any configured CredentialsProviders for the given alias. If no providers or credential is found, null is returned.
   *
   * @param conf
   *          Configuration for CredentialProvider
   * @param alias
   *          Name of CredentialEntry key
   * @return The credential if found, null otherwise
   * @throws IOException
   *           On errors reading a CredentialProvider
   */
  public static char[] getValueFromCredentialProvider(Configuration conf, String alias) throws IOException {
    Preconditions.checkNotNull(conf);
    Preconditions.checkNotNull(alias);

    if (isHadoopCredentialProviderAvailable()) {
      log.trace("Hadoop CredentialProvider is available, attempting to extract value for {}", alias);
      return getFromHadoopCredentialProvider(conf, alias);
    }

    return null;
  }

  /**
   * Attempt to extract all aliases from any configured CredentialsProviders.
   *
   * @param conf
   *          Configuration for the CredentialProvider
   * @return A list of aliases. An empty list if no CredentialProviders are configured, or the providers are empty.
   * @throws IOException
   *           On errors reading a CredentialProvider
   */
  public static List<String> getKeys(Configuration conf) throws IOException {
    Preconditions.checkNotNull(conf);

    if (isHadoopCredentialProviderAvailable()) {
      log.trace("Hadoop CredentialProvider is available, attempting to extract all aliases");
      return getAliasesFromHadoopCredentialProvider(conf);
    }

    return Collections.emptyList();
  }

  /**
   * Create a CredentialEntry using the configured Providers. If multiple CredentialProviders are configured, the first will be used.
   *
   * @param conf
   *          Configuration for the CredentialProvider
   * @param name
   *          CredentialEntry name (alias)
   * @param credential
   *          The credential
   */
  public static void createEntry(Configuration conf, String name, char[] credential) throws IOException {
    Preconditions.checkNotNull(conf);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(credential);

    if (!isHadoopCredentialProviderAvailable()) {
      log.warn("Hadoop CredentialProvider is not available");
      return;
    }

    List<Object> providers = getCredentialProviders(conf);
    if (null == providers) {
      throw new IOException("Could not fetch any CredentialProviders, is the implementation available?");
    }

    if (1 != providers.size()) {
      log.warn("Found more than one CredentialProvider. Using first provider found");
    }

    Object provider = providers.get(0);
    createEntryInProvider(provider, name, credential);
  }

  /**
   * Create a CredentialEntry with the give name and credential in the credentialProvider. The credentialProvider argument must be an instance of Hadoop
   * CredentialProvider.
   *
   * @param credentialProvider
   *          Instance of CredentialProvider
   * @param name
   *          CredentialEntry name (alias)
   * @param credential
   *          The credential to store
   */
  public static void createEntryInProvider(Object credentialProvider, String name, char[] credential) throws IOException {
    Preconditions.checkNotNull(credentialProvider);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(credential);

    if (!isHadoopCredentialProviderAvailable()) {
      log.warn("Hadoop CredentialProvider is not available");
      return;
    }

    try {
      createCredentialEntryMethod.invoke(credentialProvider, name, credential);
    } catch (IllegalArgumentException e) {
      log.warn("Failed to invoke createCredentialEntry method on CredentialProvider", e);
      return;
    } catch (IllegalAccessException e) {
      log.warn("Failed to invoke createCredentialEntry method", e);
      return;
    } catch (InvocationTargetException e) {
      log.warn("Failed to invoke createCredentialEntry method", e);
      return;
    }

    try {
      flushMethod.invoke(credentialProvider);
    } catch (IllegalArgumentException e) {
      log.warn("Failed to invoke flush method on CredentialProvider", e);
    } catch (IllegalAccessException e) {
      log.warn("Failed to invoke flush method on CredentialProvider", e);
    } catch (InvocationTargetException e) {
      log.warn("Failed to invoke flush method on CredentialProvider", e);
    }
  }
}
