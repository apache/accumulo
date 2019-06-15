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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class CredentialProviderFactoryShimTest {

  private static final Configuration hadoopConf = new Configuration();
  private static final Logger log =
      LoggerFactory.getLogger(CredentialProviderFactoryShimTest.class);

  private static final String populatedKeyStoreName = "/accumulo.jceks",
      emptyKeyStoreName = "/empty.jceks";
  private static File emptyKeyStore, populatedKeyStore;

  @BeforeClass
  public static void checkCredentialProviderAvailable() {
    try {
      Class.forName(CredentialProviderFactoryShim.HADOOP_CRED_PROVIDER_CLASS_NAME);
    } catch (Exception e) {
      // If we can't load the credential provider class, don't run the tests
      Assume.assumeNoException(e);
    }

    URL populatedKeyStoreUrl =
        CredentialProviderFactoryShimTest.class.getResource(populatedKeyStoreName),
        emptyKeyStoreUrl = CredentialProviderFactoryShimTest.class.getResource(emptyKeyStoreName);

    assertNotNull("Could not find " + populatedKeyStoreName, populatedKeyStoreUrl);
    assertNotNull("Could not find " + emptyKeyStoreName, emptyKeyStoreUrl);

    populatedKeyStore = new File(populatedKeyStoreUrl.getFile());
    emptyKeyStore = new File(emptyKeyStoreUrl.getFile());
  }

  protected String getKeyStoreUrl(File absoluteFilePath) {
    return "jceks://file" + absoluteFilePath.getAbsolutePath();
  }

  @Test(expected = NullPointerException.class)
  public void testNullConfigOnGetValue() {
    CredentialProviderFactoryShim.getValueFromCredentialProvider(null, "alias");
  }

  @Test(expected = NullPointerException.class)
  public void testNullAliasOnGetValue() {
    CredentialProviderFactoryShim.getValueFromCredentialProvider(new Configuration(false), null);
  }

  protected void checkCredentialProviders(Configuration conf, Map<String,String> expectation) {
    List<String> keys = CredentialProviderFactoryShim.getKeys(conf);
    assertNotNull(keys);

    assertEquals(expectation.keySet(), new HashSet<>(keys));
    for (String expectedKey : keys) {
      char[] value =
          CredentialProviderFactoryShim.getValueFromCredentialProvider(conf, expectedKey);
      assertNotNull(value);
      assertEquals(expectation.get(expectedKey), new String(value));
    }
  }

  @Test
  public void testExtractFromProvider() {
    String absPath = getKeyStoreUrl(populatedKeyStore);
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, absPath);
    Map<String,String> expectations = new HashMap<>();
    expectations.put("key1", "value1");
    expectations.put("key2", "value2");

    checkCredentialProviders(conf, expectations);
  }

  @Test
  public void testEmptyKeyStoreParses() {
    String absPath = getKeyStoreUrl(emptyKeyStore);
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, absPath);

    checkCredentialProviders(conf, new HashMap<>());
  }

  @Test
  public void testEmptyAndPopulatedKeyStores() {
    String populatedAbsPath = getKeyStoreUrl(populatedKeyStore),
        emptyAbsPath = getKeyStoreUrl(emptyKeyStore);
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH,
        populatedAbsPath + "," + emptyAbsPath);
    Map<String,String> expectations = new HashMap<>();
    expectations.put("key1", "value1");
    expectations.put("key2", "value2");

    checkCredentialProviders(conf, expectations);
  }

  @Test
  public void testNonExistentClassesDoesntFail() {
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, "jceks://file/foo/bar.jceks");
    List<String> keys = CredentialProviderFactoryShim.getKeys(conf);
    assertNotNull(keys);
    assertEquals(Collections.emptyList(), keys);

    assertNull(CredentialProviderFactoryShim.getValueFromCredentialProvider(conf, "key1"));
  }

  @Test
  public void testConfigurationCreation() {
    final String path = "jceks://file/tmp/foo.jks";
    final Configuration actualConf =
        CredentialProviderFactoryShim.getConfiguration(hadoopConf, path);
    assertNotNull(actualConf);
    assertEquals(path, actualConf.get(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH));
  }

  @Test
  public void createKeystoreProvider() throws Exception {
    File targetDir = new File(System.getProperty("user.dir") + "/target");
    File keystoreFile = new File(targetDir, "create.jks");
    if (keystoreFile.exists()) {
      if (!keystoreFile.delete()) {
        log.error("Unable to delete {}", keystoreFile);
      }
    }

    String providerUrl = "jceks://file" + keystoreFile.getAbsolutePath();
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, providerUrl);

    String alias = "foo";
    char[] credential = "bar".toCharArray();
    CredentialProviderFactoryShim.createEntry(conf, alias, credential);

    assertArrayEquals(credential,
        CredentialProviderFactoryShim.getValueFromCredentialProvider(conf, alias));
  }

  @Test
  public void extractFromHdfs() throws Exception {
    File target = new File(System.getProperty("user.dir"), "target");
    String prevValue = System.setProperty("test.build.data",
        new File(target, this.getClass().getName() + "_minidfs").toString());
    MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(new Configuration()).build();
    try {
      if (null != prevValue) {
        System.setProperty("test.build.data", prevValue);
      } else {
        System.clearProperty("test.build.data");
      }

      // One namenode, One configuration
      Configuration dfsConfiguration = dfsCluster.getConfiguration(0);
      Path destPath = new Path("/accumulo.jceks");
      FileSystem dfs = dfsCluster.getFileSystem();
      // Put the populated keystore in hdfs
      dfs.copyFromLocalFile(new Path(populatedKeyStore.toURI()), destPath);

      Configuration cpConf = CredentialProviderFactoryShim.getConfiguration(dfsConfiguration,
          "jceks://hdfs/accumulo.jceks");

      // The values in the keystore
      Map<String,String> expectations = new HashMap<>();
      expectations.put("key1", "value1");
      expectations.put("key2", "value2");

      checkCredentialProviders(cpConf, expectations);
    } finally {
      dfsCluster.shutdown();
    }
  }

  @Test
  public void existingConfigurationReturned() {
    Configuration conf = new Configuration(false);
    conf.set("foo", "bar");
    Configuration conf2 =
        CredentialProviderFactoryShim.getConfiguration(conf, "jceks:///file/accumulo.jceks");
    // Same object
    assertSame(conf, conf2);
    assertEquals("bar", conf.get("foo"));
  }
}
