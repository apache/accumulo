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
package org.apache.accumulo.server.conf.codec;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * EXPERIMENTAL - demonstrates using an alternate encoding scheme. The sample is completely
 * functional, however, certain elements such as password / key handling may not be suitable for
 * production. The encoding version is EXPERIMENTAL_CIPHER_ENCODING_1_0.
 * <p>
 * This codec uses AES algorithm in GCM mode for encryption to encode the property map that is
 * stored in the external store.
 */
public class VersionedPropEncryptCodec extends VersionedPropCodec {

  private static final SecureRandom random = new SecureRandom();

  // testing version (999 or higher)
  public static final int EXPERIMENTAL_CIPHER_ENCODING_1_0 = 999;

  public static final String CRYPT_ALGORITHM = "AES/GCM/NoPadding";

  private final GCMCipherParams cipherParams;

  private VersionedPropEncryptCodec(final EncodingOptions encodingOpts,
      final GCMCipherParams cipherParams) {
    super(encodingOpts);

    this.cipherParams = cipherParams;
  }

  /**
   * Instantiate a versioned property codec.
   *
   * @param compress if true, compress the payload
   * @param cipherParams the parameters needed for AES GCM encryption.
   * @return a codec for encoding / decoding versioned properties.
   */
  public static VersionedPropCodec codec(final boolean compress,
      final GCMCipherParams cipherParams) {
    return new VersionedPropEncryptCodec(
        new EncodingOptions(EXPERIMENTAL_CIPHER_ENCODING_1_0, compress), cipherParams);
  }

  @Override
  void encodePayload(final OutputStream out, final VersionedProperties vProps,
      final EncodingOptions encodingOpts) throws IOException {

    Cipher cipher;

    try {
      cipher = Cipher.getInstance(CRYPT_ALGORITHM);
      cipher.init(Cipher.ENCRYPT_MODE, cipherParams.getSecretKey(),
          cipherParams.getParameterSpec());
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
        | InvalidAlgorithmParameterException ex) {
      throw new IllegalStateException("Could not get cipher", ex);
    }

    try (DataOutputStream dos = new DataOutputStream(out)) {

      // write codec specific metadata for decryption.
      byte[] iv = cipherParams.parameterSpec.getIV();
      dos.writeInt(iv.length);
      dos.write(iv);

    }

    Map<String,String> props = vProps.asMap();

    // encode the property map to an internal byte array.
    byte[] bytes;
    if (encodingOpts.isCompressed()) {
      try (ByteArrayOutputStream ba = new ByteArrayOutputStream();
          GZIPOutputStream gzipOut = new GZIPOutputStream(ba);
          DataOutputStream dos = new DataOutputStream(gzipOut)) {

        writeMapAsUTF(dos, props);

        // finalize the compression.
        gzipOut.flush();
        gzipOut.finish();

        bytes = ba.toByteArray();
      }

    } else {
      try (ByteArrayOutputStream ba = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(ba)) {
        writeMapAsUTF(dos, props);
        bytes = ba.toByteArray();
      }
    }

    // encrypt the internal byte array and write to provided output stream
    try (CipherOutputStream cos = new CipherOutputStream(out, cipher)) {
      cos.write(bytes);
    }

  }

  @Override
  boolean checkCanDecodeVersion(EncodingOptions encodingOpts) {
    return encodingOpts.getEncodingVersion() == EXPERIMENTAL_CIPHER_ENCODING_1_0;
  }

  /**
   * Decodes the encryption specific metadata and then the map of properties. The encryption
   * metadata is the initialization vector used to encrypt the properties. The use of a random
   * initialization vector on encryption creates different encrypted values on each write even
   * though the same key is being used.
   *
   * @param inStream an input stream
   * @param encodingOpts the general encoding options.
   * @return a map of property name, value pairs.
   * @throws IOException if an error occurs reading from the input stream.
   */
  @Override
  Map<String,String> decodePayload(InputStream inStream, EncodingOptions encodingOpts)
      throws IOException {

    Cipher cipher;

    try (DataInputStream dis = new DataInputStream(inStream)) {

      // read encryption specific metadata (initialization vector)
      int ivLen = dis.readInt();
      byte[] iv = new byte[ivLen];
      int read = dis.read(iv, 0, ivLen);
      if (read != ivLen) {
        throw new IllegalStateException("Could not read data stream (reading iv array) expected "
            + ivLen + ", received " + read);
      }

      // init cipher for decryption using initialization vector just read.
      try {
        cipher = Cipher.getInstance(CRYPT_ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, cipherParams.getSecretKey(),
            GCMCipherParams.buildGCMParameterSpec(iv));
      } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
          | InvalidAlgorithmParameterException ex) {
        throw new IllegalStateException("Could not get cipher", ex);
      }

      if (encodingOpts.isCompressed()) {
        try (CipherInputStream cis = new CipherInputStream(inStream, cipher);
            GZIPInputStream gzipIn = new GZIPInputStream(cis);
            DataInputStream cdis = new DataInputStream(gzipIn)) {
          return readMapAsUTF(cdis);
        }
      } else {
        // read the property map keys, values.
        try (CipherInputStream cis = new CipherInputStream(inStream, cipher);
            DataInputStream cdis = new DataInputStream(cis)) {
          return readMapAsUTF(cdis);
        }
      }
    }
  }

  public static class GCMCipherParams {

    private final SecretKey secretKey;
    private final GCMParameterSpec parameterSpec;

    public GCMCipherParams(final char[] pass, final byte[] salt)
        throws NoSuchAlgorithmException, InvalidKeySpecException {

      SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
      KeySpec spec = new PBEKeySpec(pass, salt, 65536, 256);
      secretKey = new SecretKeySpec(factory.generateSecret(spec).getEncoded(), "AES");

      parameterSpec = buildGCMParameterSpec();
    }

    // utils
    public static GCMParameterSpec buildGCMParameterSpec() {
      byte[] iv = new byte[16];
      random.nextBytes(iv);
      return new GCMParameterSpec(128, iv);
    }

    public static GCMParameterSpec buildGCMParameterSpec(byte[] iv) {
      return new GCMParameterSpec(128, iv);
    }

    public SecretKey getSecretKey() {
      return secretKey;
    }

    public GCMParameterSpec getParameterSpec() {
      return parameterSpec;
    }

  }

}
