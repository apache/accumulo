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
package org.apache.accumulo.test.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.SignatureException;
import java.security.cert.Certificate;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CertUtilsTest {
  private static final String KEYSTORE_TYPE = "JKS";
  private static final String PASSWORD = "CertUtilsTestPassword";
  private static final char[] PASSWORD_CHARS = PASSWORD.toCharArray();
  private static final String RDN_STRING = "o=Apache Accumulo,cn=CertUtilsTest";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  private CertUtils getUtils() {
    return new CertUtils(KEYSTORE_TYPE, RDN_STRING, "RSA", 2048, "sha1WithRSAEncryption");
  }

  @Test
  public void createSelfSigned() throws Exception {
    CertUtils certUtils = getUtils();
    File keyStoreFile = new File(folder.getRoot(), "selfsigned.jks");
    certUtils.createSelfSignedCert(keyStoreFile, "test", PASSWORD);

    KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (FileInputStream fis = new FileInputStream(keyStoreFile)) {
      keyStore.load(fis, PASSWORD_CHARS);
    }
    Certificate cert = CertUtils.findCert(keyStore);

    cert.verify(cert.getPublicKey()); // throws exception if it can't be verified
  }

  @Test
  public void createPublicSelfSigned() throws Exception {
    CertUtils certUtils = getUtils();
    File rootKeyStoreFile = new File(folder.getRoot(), "root.jks");
    certUtils.createSelfSignedCert(rootKeyStoreFile, "test", PASSWORD);
    File publicKeyStoreFile = new File(folder.getRoot(), "public.jks");
    certUtils.createPublicCert(publicKeyStoreFile, "test", rootKeyStoreFile.getAbsolutePath(), PASSWORD, "");

    KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (FileInputStream fis = new FileInputStream(publicKeyStoreFile)) {
      keyStore.load(fis, new char[0]);
    }
    try {
      CertUtils.findPrivateKey(keyStore, PASSWORD_CHARS);
      fail("expected not to find private key in keystore");
    } catch (KeyStoreException e) {
      assertTrue(e.getMessage().contains("private key"));
    }
    Certificate cert = CertUtils.findCert(keyStore);
    cert.verify(cert.getPublicKey()); // throws exception if it can't be verified
  }

  @Test
  public void createSigned() throws Exception {
    CertUtils certUtils = getUtils();
    File rootKeyStoreFile = new File(folder.getRoot(), "root.jks");
    certUtils.createSelfSignedCert(rootKeyStoreFile, "test", PASSWORD);
    File signedKeyStoreFile = new File(folder.getRoot(), "signed.jks");
    certUtils.createSignedCert(signedKeyStoreFile, "test", PASSWORD, rootKeyStoreFile.getAbsolutePath(), PASSWORD);

    KeyStore rootKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (FileInputStream fis = new FileInputStream(rootKeyStoreFile)) {
      rootKeyStore.load(fis, PASSWORD_CHARS);
    }
    Certificate rootCert = CertUtils.findCert(rootKeyStore);

    KeyStore signedKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (FileInputStream fis = new FileInputStream(signedKeyStoreFile)) {
      signedKeyStore.load(fis, PASSWORD_CHARS);
    }
    Certificate signedCert = CertUtils.findCert(signedKeyStore);

    try {
      signedCert.verify(signedCert.getPublicKey());
      fail("signed cert should not be able to verify itself");
    } catch (SignatureException e) {
      // expected
    }

    signedCert.verify(rootCert.getPublicKey()); // throws exception if it can't be verified
  }

  @Test
  public void publicOnlyVerfication() throws Exception {
    // this approximates the real life scenario. the client will only have the public key of each
    // cert (the root made by us as below, but the signed cert extracted by the SSL transport)
    CertUtils certUtils = getUtils();
    File rootKeyStoreFile = new File(folder.getRoot(), "root.jks");
    certUtils.createSelfSignedCert(rootKeyStoreFile, "test", PASSWORD);
    File publicRootKeyStoreFile = new File(folder.getRoot(), "publicroot.jks");
    certUtils.createPublicCert(publicRootKeyStoreFile, "test", rootKeyStoreFile.getAbsolutePath(), PASSWORD, "");
    File signedKeyStoreFile = new File(folder.getRoot(), "signed.jks");
    certUtils.createSignedCert(signedKeyStoreFile, "test", PASSWORD, rootKeyStoreFile.getAbsolutePath(), PASSWORD);
    File publicSignedKeyStoreFile = new File(folder.getRoot(), "publicsigned.jks");
    certUtils.createPublicCert(publicSignedKeyStoreFile, "test", signedKeyStoreFile.getAbsolutePath(), PASSWORD, "");

    KeyStore rootKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (FileInputStream fis = new FileInputStream(publicRootKeyStoreFile)) {
      rootKeyStore.load(fis, new char[0]);
    }
    KeyStore signedKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (FileInputStream fis = new FileInputStream(publicSignedKeyStoreFile)) {
      signedKeyStore.load(fis, new char[0]);
    }
    Certificate rootCert = CertUtils.findCert(rootKeyStore);
    Certificate signedCert = CertUtils.findCert(signedKeyStore);

    try {
      signedCert.verify(signedCert.getPublicKey());
      fail("signed cert should not be able to verify itself");
    } catch (SignatureException e) {
      // expected
    }

    signedCert.verify(rootCert.getPublicKey()); // throws exception if it can't be verified
  }

  @Test
  public void signingChain() throws Exception {
    // no reason the keypair we generate for the tservers need to be able to sign anything,
    // but this is a way to make sure the private and public keys created actually correspond.
    CertUtils certUtils = getUtils();
    File rootKeyStoreFile = new File(folder.getRoot(), "root.jks");
    certUtils.createSelfSignedCert(rootKeyStoreFile, "test", PASSWORD);
    File signedCaKeyStoreFile = new File(folder.getRoot(), "signedca.jks");
    certUtils.createSignedCert(signedCaKeyStoreFile, "test", PASSWORD, rootKeyStoreFile.getAbsolutePath(), PASSWORD);
    File signedLeafKeyStoreFile = new File(folder.getRoot(), "signedleaf.jks");
    certUtils.createSignedCert(signedLeafKeyStoreFile, "test", PASSWORD, signedCaKeyStoreFile.getAbsolutePath(), PASSWORD);

    KeyStore caKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (FileInputStream fis = new FileInputStream(signedCaKeyStoreFile)) {
      caKeyStore.load(fis, PASSWORD_CHARS);
    }
    Certificate caCert = CertUtils.findCert(caKeyStore);

    KeyStore leafKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    try (FileInputStream fis = new FileInputStream(signedLeafKeyStoreFile)) {
      leafKeyStore.load(fis, PASSWORD_CHARS);
    }
    Certificate leafCert = CertUtils.findCert(leafKeyStore);

    leafCert.verify(caCert.getPublicKey()); // throws exception if it can't be verified
  }
}
