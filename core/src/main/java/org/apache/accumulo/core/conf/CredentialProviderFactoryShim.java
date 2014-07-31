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

import jline.internal.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shim around Hadoop: tries to use the CredentialProviderFactory provided by hadoop-common, falling back to a copy inside accumulo-core.
 * <p>
 * The CredentialProvider classes only exist in 2.6.0, so, to use them, we have to do a bunch of reflection. This will also help us to continue to support
 * [2.2.0,2.6.0) when 2.6.0 is officially released.
 */
public class CredentialProviderFactoryShim {
  private static final Logger log = LoggerFactory.getLogger(CredentialProviderFactoryShim.class);

  protected static final String HADOOP_CRED_PROVIDER_FACTORY_CLASS_NAME = "org.apache.hadoop.security.alias.JavaKeyStoreProvider$Factory";
  protected static final String HADOOP_CRED_PROVIDER_FACTORY_GET_PROVIDERS_METHOD_NAME = "getProviders";

  protected static final String HADOOP_CRED_PROVIDER_CLASS_NAME = "org.apache.hadoop.security.alias.CredentialProvider";
  protected static final String HADOOP_CRED_PROVIDER_GET_CREDENTIAL_ENTRY_METHOD_NAME = "getCredentialEntry";
  protected static final String HADOOP_CRED_PROVIDER_GET_ALIASES_METHOD_NAME = "getAliases";

  protected static final String HADOOP_CRED_ENTRY_CLASS_NAME = "org.apache.hadoop.security.alias.CredentialProvider$CredentialEntry";
  protected static final String HADOOP_CRED_ENTRY_GET_CREDENTIAL_METHOD_NAME = "getCredential";

  public static final String CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";

  private static Object hadoopCredProviderFactory = null;
  private static Method getProvidersMethod = null;
  private static Method getAliasesMethod = null;
  private static Method getCredentialEntryMethod = null;
  private static Method getCredentialMethod = null;
  private static Boolean hadoopClassesAvailable = null;

  /**
   * Determine if we can load the necessary CredentialProvider classes. Only loaded the first time, so subsequent invocations of this method should return fast.
   *
   * @return True if the CredentialProvider classes/methods are available, false otherwise.
   */
  protected static synchronized boolean isHadoopCredentialProviderAvailable() {
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
        log.warn("Failed to get credential from {}", providerObj, e);
        continue;
      } catch (IllegalAccessException e) {
        log.warn("Failed to get credential from {}", providerObj, e);
        continue;
      } catch (InvocationTargetException e) {
        log.warn("Failed to get credential from {}", providerObj, e);
        continue;
      }
    }

    log.warn("Could not extract credential from providers");

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
}
