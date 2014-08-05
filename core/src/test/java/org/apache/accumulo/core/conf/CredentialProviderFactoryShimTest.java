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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 */
public class CredentialProviderFactoryShimTest {

  private static boolean isCredentialProviderAvailable = false;
  private static final String populatedKeyStoreName = "/accumulo.jceks", emptyKeyStoreName = "/empty.jceks";
  private static File emptyKeyStore, populatedKeyStore;

  @BeforeClass
  public static void checkCredentialProviderAvailable() {
    try {
      Class.forName(CredentialProviderFactoryShim.HADOOP_CRED_PROVIDER_CLASS_NAME);
      isCredentialProviderAvailable = true;
    } catch (Exception e) {
      isCredentialProviderAvailable = false;
    }

    if (isCredentialProviderAvailable) {
      URL populatedKeyStoreUrl = CredentialProviderFactoryShimTest.class.getResource(populatedKeyStoreName),
          emptyKeyStoreUrl = CredentialProviderFactoryShimTest.class.getResource(emptyKeyStoreName); 

      Assert.assertNotNull("Could not find " + populatedKeyStoreName, populatedKeyStoreUrl);
      Assert.assertNotNull("Could not find " + emptyKeyStoreName, emptyKeyStoreUrl);

      populatedKeyStore = new File(populatedKeyStoreUrl.getFile());
      emptyKeyStore = new File(emptyKeyStoreUrl.getFile());
    }
  }

  protected String getKeyStoreUrl(File absoluteFilePath) {
    return "jceks://file" + absoluteFilePath.getAbsolutePath();
  }

  @Test(expected = NullPointerException.class)
  public void testNullConfigOnGetValue() throws IOException {
    CredentialProviderFactoryShim.getValueFromCredentialProvider(null, "alias");
  }

  @Test(expected = NullPointerException.class)
  public void testNullAliasOnGetValue() throws IOException {
    CredentialProviderFactoryShim.getValueFromCredentialProvider(new Configuration(false), null);
  }

  protected void checkCredentialProviders(Configuration conf, Map<String,String> expectation) throws IOException {
    List<String> keys = CredentialProviderFactoryShim.getKeys(conf);
    Assert.assertNotNull(keys);
    
    Assert.assertEquals(expectation.keySet(), new HashSet<String>(keys));
    for (String expectedKey : keys) {
      char[] value = CredentialProviderFactoryShim.getValueFromCredentialProvider(conf, expectedKey);
      Assert.assertNotNull(value);
      Assert.assertEquals(expectation.get(expectedKey), new String(value));
    }
  }

  @Test
  public void testExtractFromProvider() throws IOException {
    if (!isCredentialProviderAvailable) {
      return;
    }

    String absPath = getKeyStoreUrl(populatedKeyStore);
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, absPath);
    Map<String,String> expectations = new HashMap<String,String>();
    expectations.put("key1", "value1");
    expectations.put("key2", "value2");

    checkCredentialProviders(conf, expectations);
  }

  @Test
  public void testEmptyKeyStoreParses() throws IOException {
    if (!isCredentialProviderAvailable) {
      return;
    }

    String absPath = getKeyStoreUrl(emptyKeyStore);
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, absPath);

    checkCredentialProviders(conf, new HashMap<String,String>());
  }

  @Test
  public void testEmptyAndPopulatedKeyStores() throws IOException {

    if (!isCredentialProviderAvailable) {
      return;
    }

    String populatedAbsPath = getKeyStoreUrl(populatedKeyStore), emptyAbsPath = getKeyStoreUrl(emptyKeyStore);
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, populatedAbsPath + "," + emptyAbsPath);
    Map<String,String> expectations = new HashMap<String,String>();
    expectations.put("key1", "value1");
    expectations.put("key2", "value2");

    checkCredentialProviders(conf, expectations);
  }

  @Test
  public void testNonExistentClassesDoesntFail() throws IOException {
    if (isCredentialProviderAvailable) {
      return;
    }

    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, "jceks://file/foo/bar.jceks");
    List<String> keys = CredentialProviderFactoryShim.getKeys(conf);
    Assert.assertNotNull(keys);
    Assert.assertEquals(Collections.emptyList(), keys);

    Assert.assertNull(CredentialProviderFactoryShim.getValueFromCredentialProvider(conf, "key1"));
  }

  @Test
  public void testConfigurationCreation() throws IOException {
    final String path = "jceks://file/tmp/foo.jks";
    final Configuration actualConf = CredentialProviderFactoryShim.getConfiguration(path);
    Assert.assertNotNull(actualConf);
    Assert.assertEquals(path, actualConf.get(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH));
  }
}
