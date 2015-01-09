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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.commons.io.FileExistsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.IETFUtils;
import org.bouncycastle.asn1.x500.style.RFC4519Style;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.provider.X509CertificateObject;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class CertUtils {
  private static final Logger log = Logger.getLogger(CertUtils.class);
  static {
    Security.addProvider(new BouncyCastleProvider());
  }

  static class Opts extends Help {
    @Parameter(description = "generate-all | generate-local | generate-self-trusted", required = true, arity = 1)
    List<String> operation = null;

    @Parameter(names = {"--local-keystore"}, description = "Target path for generated keystore")
    String localKeystore = null;

    @Parameter(names = {"--root-keystore"}, description = "Path to root truststore, generated with generate-all, or used for signing with generate-local")
    String rootKeystore = null;

    @Parameter(names = {"--root-truststore"}, description = "Target path for generated public root truststore")
    String truststore = null;

    @Parameter(names = {"--keystore-type"}, description = "Type of keystore file to use")
    String keystoreType = "JKS";

    @Parameter(names = {"--root-keystore-password"}, description = "Password for root keystore, falls back to --keystore-password if not provided")
    String rootKeystorePassword = null;

    @Parameter(
        names = {"--keystore-password"},
        description = "Password used to encrypt keystores.  If omitted, the instance-wide secret will be used.  If specified, the password must also be explicitly configured in Accumulo.")
    String keystorePassword = null;

    @Parameter(names = {"--truststore-password"}, description = "Password used to encrypt the truststore. If omitted, empty password is used")
    String truststorePassword = "";

    @Parameter(names = {"--key-name-prefix"}, description = "Prefix for names of generated keys")
    String keyNamePrefix = CertUtils.class.getSimpleName();

    @Parameter(names = {"--issuer-rdn"}, description = "RDN string for issuer, for example: 'c=US,o=My Organization,cn=My Name'")
    String issuerDirString = "o=Apache Accumulo";

    @Parameter(names = "--site-file", description = "Load configuration from the given site file")
    public String siteFile = null;

    @Parameter(names = "--signing-algorithm", description = "Algorithm used to sign certificates")
    public String signingAlg = "SHA256WITHRSA";

    @Parameter(names = "--encryption-algorithm", description = "Algorithm used to encrypt private keys")
    public String encryptionAlg = "RSA";

    @Parameter(names = "--keysize", description = "Key size used by encryption algorithm")
    public int keysize = 2048;

    public AccumuloConfiguration getConfiguration() {
      if (siteFile == null) {
        return SiteConfiguration.getInstance(DefaultConfiguration.getInstance());
      } else {
        return new AccumuloConfiguration() {
          Configuration xml = new Configuration();
          {
            xml.addResource(new Path(siteFile));
          }

          @Override
          public Iterator<Entry<String,String>> iterator() {
            TreeMap<String,String> map = new TreeMap<String,String>();
            for (Entry<String,String> props : DefaultConfiguration.getInstance())
              map.put(props.getKey(), props.getValue());
            for (Entry<String,String> props : xml)
              map.put(props.getKey(), props.getValue());
            return map.entrySet().iterator();
          }

          @Override
          public String get(Property property) {
            String value = xml.get(property.getKey());
            if (value != null)
              return value;
            return DefaultConfiguration.getInstance().get(property);
          }

          @Override
          public void getProperties(Map<String,String> props, PropertyFilter filter) {
            for (Entry<String,String> entry : this)
              if (filter.accept(entry.getKey()))
                props.put(entry.getKey(), entry.getValue());
          }
        };
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(CertUtils.class.getName(), args);
    String operation = opts.operation.get(0);

    String keyPassword = opts.keystorePassword;
    if (keyPassword == null)
      keyPassword = getDefaultKeyPassword();

    String rootKeyPassword = opts.rootKeystorePassword;
    if (rootKeyPassword == null) {
      rootKeyPassword = keyPassword;
    }

    CertUtils certUtils = new CertUtils(opts.keystoreType, opts.issuerDirString, opts.encryptionAlg, opts.keysize, opts.signingAlg);

    if ("generate-all".equals(operation)) {
      certUtils.createAll(new File(opts.rootKeystore), new File(opts.localKeystore), new File(opts.truststore), opts.keyNamePrefix, rootKeyPassword,
          keyPassword, opts.truststorePassword);
    } else if ("generate-local".equals(operation)) {
      certUtils.createSignedCert(new File(opts.localKeystore), opts.keyNamePrefix + "-local", keyPassword, opts.rootKeystore, rootKeyPassword);
    } else if ("generate-self-trusted".equals(operation)) {
      certUtils.createSelfSignedCert(new File(opts.truststore), opts.keyNamePrefix + "-selfTrusted", keyPassword);
    } else {
      JCommander jcommander = new JCommander(opts);
      jcommander.setProgramName(CertUtils.class.getName());
      jcommander.usage();
      System.err.println("Unrecognized operation: " + opts.operation);
      System.exit(0);
    }
  }

  private static String getDefaultKeyPassword() {
    return SiteConfiguration.getInstance(DefaultConfiguration.getInstance()).get(Property.INSTANCE_SECRET);
  }

  private String issuerDirString;
  private String keystoreType;
  private String encryptionAlgorithm;
  private int keysize;
  private String signingAlgorithm;

  public CertUtils(String keystoreType, String issuerDirString, String encryptionAlgorithm, int keysize, String signingAlgorithm) {
    super();
    this.keystoreType = keystoreType;
    this.issuerDirString = issuerDirString;
    this.encryptionAlgorithm = encryptionAlgorithm;
    this.keysize = keysize;
    this.signingAlgorithm = signingAlgorithm;
  }

  public void createAll(File rootKeystoreFile, File localKeystoreFile, File trustStoreFile, String keyNamePrefix, String rootKeystorePassword,
      String keystorePassword, String truststorePassword) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException,
      OperatorCreationException, AccumuloSecurityException, NoSuchProviderException, UnrecoverableKeyException, FileNotFoundException {
    createSelfSignedCert(rootKeystoreFile, keyNamePrefix + "-root", rootKeystorePassword);
    createSignedCert(localKeystoreFile, keyNamePrefix + "-local", keystorePassword, rootKeystoreFile.getAbsolutePath(), rootKeystorePassword);
    createPublicCert(trustStoreFile, keyNamePrefix + "-public", rootKeystoreFile.getAbsolutePath(), rootKeystorePassword, truststorePassword);
  }

  public void createPublicCert(File targetKeystoreFile, String keyName, String rootKeystorePath, String rootKeystorePassword, String truststorePassword)
      throws NoSuchAlgorithmException, CertificateException, FileNotFoundException, IOException, KeyStoreException, UnrecoverableKeyException {
    KeyStore signerKeystore = KeyStore.getInstance(keystoreType);
    char[] signerPasswordArray = rootKeystorePassword.toCharArray();
    signerKeystore.load(new FileInputStream(rootKeystorePath), signerPasswordArray);
    Certificate rootCert = findCert(signerKeystore);

    KeyStore keystore = KeyStore.getInstance(keystoreType);
    keystore.load(null, null);
    keystore.setCertificateEntry(keyName + "Cert", rootCert);
    keystore.store(new FileOutputStream(targetKeystoreFile), truststorePassword.toCharArray());
  }

  public void createSignedCert(File targetKeystoreFile, String keyName, String keystorePassword, String signerKeystorePath, String signerKeystorePassword)
      throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException, OperatorCreationException, AccumuloSecurityException,
      UnrecoverableKeyException, NoSuchProviderException {
    KeyStore signerKeystore = KeyStore.getInstance(keystoreType);
    char[] signerPasswordArray = signerKeystorePassword.toCharArray();
    signerKeystore.load(new FileInputStream(signerKeystorePath), signerPasswordArray);
    Certificate signerCert = findCert(signerKeystore);
    PrivateKey signerKey = findPrivateKey(signerKeystore, signerPasswordArray);

    KeyPair kp = generateKeyPair();
    X509CertificateObject cert = generateCert(keyName, kp, false, signerCert.getPublicKey(), signerKey);

    char[] password = keystorePassword.toCharArray();
    KeyStore keystore = KeyStore.getInstance(keystoreType);
    keystore.load(null, null);
    keystore.setCertificateEntry(keyName + "Cert", cert);
    keystore.setKeyEntry(keyName + "Key", kp.getPrivate(), password, new Certificate[] {cert, signerCert});
    keystore.store(new FileOutputStream(targetKeystoreFile), password);
  }

  public void createSelfSignedCert(File targetKeystoreFile, String keyName, String keystorePassword) throws KeyStoreException, CertificateException,
      NoSuchAlgorithmException, IOException, OperatorCreationException, AccumuloSecurityException, NoSuchProviderException {
    if (targetKeystoreFile.exists()) {
      throw new FileExistsException(targetKeystoreFile);
    }

    KeyPair kp = generateKeyPair();

    X509CertificateObject cert = generateCert(keyName, kp, true, kp.getPublic(), kp.getPrivate());

    char[] password = keystorePassword.toCharArray();
    KeyStore keystore = KeyStore.getInstance(keystoreType);
    keystore.load(null, null);
    keystore.setCertificateEntry(keyName + "Cert", cert);
    keystore.setKeyEntry(keyName + "Key", kp.getPrivate(), password, new Certificate[] {cert});
    keystore.store(new FileOutputStream(targetKeystoreFile), password);
  }

  private KeyPair generateKeyPair() throws NoSuchAlgorithmException, NoSuchProviderException {
    KeyPairGenerator gen = KeyPairGenerator.getInstance(encryptionAlgorithm);
    gen.initialize(keysize);
    return gen.generateKeyPair();
  }

  private X509CertificateObject generateCert(String keyName, KeyPair kp, boolean isCertAuthority, PublicKey signerPublicKey, PrivateKey signerPrivateKey)
      throws IOException, CertIOException, OperatorCreationException, CertificateException, NoSuchAlgorithmException {
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    endDate.add(Calendar.YEAR, 100);

    BigInteger serialNumber = BigInteger.valueOf((startDate.getTimeInMillis()));
    X500Name issuer = new X500Name(IETFUtils.rDNsFromString(issuerDirString, RFC4519Style.INSTANCE));
    JcaX509v3CertificateBuilder certGen = new JcaX509v3CertificateBuilder(issuer, serialNumber, startDate.getTime(), endDate.getTime(), issuer, kp.getPublic());
    JcaX509ExtensionUtils extensionUtils = new JcaX509ExtensionUtils();
    certGen.addExtension(Extension.subjectKeyIdentifier, false, extensionUtils.createSubjectKeyIdentifier(kp.getPublic()));
    certGen.addExtension(Extension.basicConstraints, false, new BasicConstraints(isCertAuthority));
    certGen.addExtension(Extension.authorityKeyIdentifier, false, extensionUtils.createAuthorityKeyIdentifier(signerPublicKey));
    if (isCertAuthority) {
      certGen.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign));
    }
    X509CertificateHolder cert = certGen.build(new JcaContentSignerBuilder(signingAlgorithm).build(signerPrivateKey));
    return new X509CertificateObject(cert.toASN1Structure());
  }

  static Certificate findCert(KeyStore keyStore) throws KeyStoreException {
    Enumeration<String> aliases = keyStore.aliases();
    Certificate cert = null;
    while (aliases.hasMoreElements()) {
      String alias = aliases.nextElement();
      if (keyStore.isCertificateEntry(alias)) {
        if (cert == null) {
          cert = keyStore.getCertificate(alias);
        } else {
          log.warn("Found multiple certificates in keystore.  Ignoring " + alias);
        }
      }
    }
    if (cert == null) {
      throw new KeyStoreException("Could not find cert in keystore");
    }
    return cert;
  }

  static PrivateKey findPrivateKey(KeyStore keyStore, char[] keystorePassword) throws UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException {
    Enumeration<String> aliases = keyStore.aliases();
    PrivateKey key = null;
    while (aliases.hasMoreElements()) {
      String alias = aliases.nextElement();
      if (keyStore.isKeyEntry(alias)) {
        if (key == null) {
          key = (PrivateKey) keyStore.getKey(alias, keystorePassword);
        } else {
          log.warn("Found multiple keys in keystore.  Ignoring " + alias);
        }
      }
    }
    if (key == null) {
      throw new KeyStoreException("Could not find private key in keystore");
    }
    return key;
  }
}
