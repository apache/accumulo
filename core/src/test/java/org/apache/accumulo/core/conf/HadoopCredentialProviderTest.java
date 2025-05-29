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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class HadoopCredentialProviderTest {

  @TempDir
  private static java.nio.file.Path tempDir;

  private static final Configuration hadoopConf = new Configuration();
  private static final Logger log = LoggerFactory.getLogger(HadoopCredentialProviderTest.class);

  private static final String populatedKeyStoreName = "/accumulo.jceks",
      emptyKeyStoreName = "/empty.jceks";
  private static File emptyKeyStore, populatedKeyStore;

  @BeforeAll
  public static void checkCredentialProviderAvailable() throws Exception {
    URL populatedKeyStoreUrl =
        HadoopCredentialProviderTest.class.getResource(populatedKeyStoreName),
        emptyKeyStoreUrl = HadoopCredentialProviderTest.class.getResource(emptyKeyStoreName);

    assertNotNull(populatedKeyStoreUrl, "Could not find " + populatedKeyStoreName);
    assertNotNull(emptyKeyStoreUrl, "Could not find " + emptyKeyStoreName);

    populatedKeyStore = java.nio.file.Path.of(populatedKeyStoreUrl.toURI()).toFile();
    emptyKeyStore = java.nio.file.Path.of(emptyKeyStoreUrl.toURI()).toFile();
  }

  protected String getKeyStoreUrl(File absoluteFilePath) {
    return "jceks://file" + absoluteFilePath.getAbsolutePath();
  }

  @Test
  public void testNullConfigOnGetValue() {
    assertThrows(NullPointerException.class,
        () -> HadoopCredentialProvider.getValue(null, "alias"));
  }

  @Test
  public void testNullAliasOnGetValue() {
    assertThrows(NullPointerException.class,
        () -> HadoopCredentialProvider.getValue(new Configuration(false), null));
  }

  protected void checkCredentialProviders(Configuration conf, Map<String,String> expectation) {
    List<String> keys = HadoopCredentialProvider.getKeys(conf);
    assertNotNull(keys);

    assertEquals(expectation.keySet(), new HashSet<>(keys));
    for (String expectedKey : keys) {
      char[] value = HadoopCredentialProvider.getValue(conf, expectedKey);
      assertNotNull(value);
      assertEquals(expectation.get(expectedKey), new String(value));
    }
  }

  @Test
  public void testExtractFromProvider() {
    String absPath = getKeyStoreUrl(populatedKeyStore);
    Configuration conf = new Configuration();
    HadoopCredentialProvider.setPath(conf, absPath);
    Map<String,String> expectations = new HashMap<>();
    expectations.put("key1", "value1");
    expectations.put("key2", "value2");

    checkCredentialProviders(conf, expectations);
  }

  @Test
  public void testEmptyKeyStoreParses() {
    String absPath = getKeyStoreUrl(emptyKeyStore);
    Configuration conf = new Configuration();
    HadoopCredentialProvider.setPath(conf, absPath);

    checkCredentialProviders(conf, new HashMap<>());
  }

  @Test
  public void testEmptyAndPopulatedKeyStores() {
    String populatedAbsPath = getKeyStoreUrl(populatedKeyStore),
        emptyAbsPath = getKeyStoreUrl(emptyKeyStore);
    Configuration conf = new Configuration();
    HadoopCredentialProvider.setPath(conf, populatedAbsPath + "," + emptyAbsPath);
    Map<String,String> expectations = new HashMap<>();
    expectations.put("key1", "value1");
    expectations.put("key2", "value2");

    checkCredentialProviders(conf, expectations);
  }

  @Test
  public void testNonExistentClassesDoesntFail() {
    Configuration conf = new Configuration();
    HadoopCredentialProvider.setPath(conf, "jceks://file/foo/bar.jceks");
    List<String> keys = HadoopCredentialProvider.getKeys(conf);
    assertNotNull(keys);
    assertEquals(Collections.emptyList(), keys);

    assertNull(HadoopCredentialProvider.getValue(conf, "key1"));
  }

  @Test
  public void testConfigurationCreation() {
    final String path = "jceks://file/tmp/foo.jks";
    final Configuration actualConf = hadoopConf;
    HadoopCredentialProvider.setPath(actualConf, path);
    assertNotNull(actualConf);
    assertEquals(path, actualConf.get("hadoop.security.credential.provider.path"));
  }

  @Test
  public void createKeystoreProvider() throws Exception {
    java.nio.file.Path keystoreFile = tempDir.resolve("create.jks");
    Files.deleteIfExists(keystoreFile);

    String providerUrl = "jceks://file" + keystoreFile.toAbsolutePath();
    Configuration conf = new Configuration();
    HadoopCredentialProvider.setPath(conf, providerUrl);

    String alias = "foo";
    char[] credential = "bar".toCharArray();
    HadoopCredentialProvider.createEntry(conf, alias, credential);

    assertArrayEquals(credential, HadoopCredentialProvider.getValue(conf, alias));
  }

  @Test
  public void extractFromHdfs() throws Exception {
    String prevValue = System.setProperty("test.build.data",
        tempDir.resolve(this.getClass().getName() + "_minidfs").toString());
    try (MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(new Configuration()).build()) {
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

      Configuration cpConf = dfsConfiguration;
      HadoopCredentialProvider.setPath(cpConf, "jceks://hdfs/accumulo.jceks");

      // The values in the keystore
      Map<String,String> expectations = new HashMap<>();
      expectations.put("key1", "value1");
      expectations.put("key2", "value2");

      checkCredentialProviders(cpConf, expectations);
    }
  }

}
