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
package org.apache.accumulo.core.security.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * Classes that obey this interface may be used to provide encrypting and decrypting streams to the rest of Accumulo. Classes that obey this interface may be
 * configured as the crypto module by setting the property crypto.module.class in the accumulo-site.xml file.
 * 
 * Note that this first iteration of this API is considered deprecated because we anticipate it changing in non-backwards compatible ways as we explore the
 * requirements for encryption in Accumulo. So, your mileage is gonna vary a lot as we go forward.
 * 
 */
@Deprecated
public interface CryptoModule {
  
  public enum CryptoInitProperty {
    ALGORITHM_NAME("algorithm.name"), CIPHER_SUITE("cipher.suite"), INITIALIZATION_VECTOR("initialization.vector"), PLAINTEXT_SESSION_KEY(
        "plaintext.session.key");
    
    private CryptoInitProperty(String name) {
      key = name;
    }
    
    private String key;
    
    public String getKey() {
      return key;
    }
  }
  
  /**
   * Wraps an OutputStream in an encrypting OutputStream. The given map contains the settings for the cryptographic algorithm to use. <b>Callers of this method
   * should expect that the given OutputStream will be written to before cryptographic writes occur.</b> These writes contain the cryptographic information used
   * to encrypt the following bytes (these data include the initialization vector, encrypted session key, and so on). If writing arbitrarily to the underlying
   * stream is not desirable, users should call the other flavor of getEncryptingOutputStream which accepts these data as parameters.
   * 
   * @param out
   *          the OutputStream to wrap
   * @param cryptoOpts
   *          the cryptographic parameters to use; specific string names to look for will depend on the various implementations
   * @return an OutputStream that wraps the given parameter
   * @throws IOException
   */
  public OutputStream getEncryptingOutputStream(OutputStream out, Map<String,String> cryptoOpts) throws IOException;
  
  /**
   * Wraps an InputStream and returns a decrypting input stream. The given map contains the settings for the intended cryptographic operations, but implementors
   * should take care to ensure that the crypto from the given input stream matches their expectations about what they will use to decrypt it, as the parameters
   * may have changed. Also, care should be taken around transitioning between non-encrypting and encrypting streams; implementors should handle the case where
   * the given input stream is <b>not</b> encrypted at all.
   * 
   * It is expected that this version of getDecryptingInputStream is called in conjunction with the getEncryptingOutputStream from above. It should expect its
   * input streams to contain the data written by getEncryptingOutputStream.
   * 
   * @param in
   *          the InputStream to wrap
   * @param cryptoOpts
   *          the cryptographic parameters to use; specific string names to look for will depend on the various implementations
   * @return an InputStream that wraps the given parameter
   * @throws IOException
   */
  public InputStream getDecryptingInputStream(InputStream in, Map<String,String> cryptoOpts) throws IOException;
  
  /**
   * Wraps an OutputStream in an encrypting OutputStream. The given map contains the settings for the cryptographic algorithm to use. The cryptoInitParams map
   * contains all the cryptographic details to construct a key (or keys), initialization vectors, etc. and use them to properly initialize the stream for
   * writing. These initialization parameters must be persisted elsewhere, along with the cryptographic configuration (algorithm, mode, etc.), so that they may
   * be read in at the time of reading the encrypted content.
   * 
   * @param out
   *          the OutputStream to wrap
   * @param conf
   *          the cryptographic algorithm configuration
   * @param cryptoInitParams
   *          the initialization parameters for the algorithm, usually including initialization vector and session key
   * @return a wrapped output stream
   */
  public OutputStream getEncryptingOutputStream(OutputStream out, Map<String,String> conf, Map<CryptoModule.CryptoInitProperty,Object> cryptoInitParams);
  
  /**
   * Wraps an InputStream and returns a decrypting input stream. The given map contains the settings for the intended cryptographic operations, but implementors
   * should take care to ensure that the crypto from the given input stream matches their expectations about what they will use to decrypt it, as the parameters
   * may have changed. Also, care should be taken around transitioning between non-encrypting and encrypting streams; implementors should handle the case where
   * the given input stream is <b>not</b> encrypted at all.
   * 
   * The cryptoInitParams contains all necessary information to properly initialize the given cipher, usually including things like initialization vector and
   * secret key.
   */
  public InputStream getDecryptingInputStream(InputStream in, Map<String,String> cryptoOpts, Map<CryptoModule.CryptoInitProperty,Object> cryptoInitParams)
      throws IOException;
}
