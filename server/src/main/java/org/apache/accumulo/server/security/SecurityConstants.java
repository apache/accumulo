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
package org.apache.accumulo.server.security;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecurityPermission;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

public class SecurityConstants {
  private static SecurityPermission SYSTEM_CREDENTIALS_PERMISSION = new SecurityPermission("systemCredentialsPermission");
  static Logger log = Logger.getLogger(SecurityConstants.class);
  
  public static final String SYSTEM_PRINCIPAL = "!SYSTEM";
  private static final AuthenticationToken SYSTEM_TOKEN = makeSystemPassword();
  private static final TCredentials systemCredentials = CredentialHelper.createSquelchError(SYSTEM_PRINCIPAL, SYSTEM_TOKEN, HdfsZooInstance.getInstance()
      .getInstanceID());
  public static byte[] confChecksum = null;
  
  public static AuthenticationToken getSystemToken() {
    return SYSTEM_TOKEN;
  }
  
  public static TCredentials getSystemCredentials() {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(SYSTEM_CREDENTIALS_PERMISSION);
    }
    return systemCredentials;
  }
  
  public static String getSystemPrincipal() {
    return SYSTEM_PRINCIPAL;
  }
  
  private static AuthenticationToken makeSystemPassword() {
    int wireVersion = Constants.WIRE_VERSION;
    byte[] inst = HdfsZooInstance.getInstance().getInstanceID().getBytes(Constants.UTF8);
    try {
      confChecksum = getSystemConfigChecksum();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Failed to compute configuration checksum", e);
    }
    
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(3 * (Integer.SIZE / Byte.SIZE) + inst.length + confChecksum.length);
    DataOutputStream out = new DataOutputStream(bytes);
    try {
      out.write(wireVersion * -1);
      out.write(inst.length);
      out.write(inst);
      out.write(confChecksum.length);
      out.write(confChecksum);
    } catch (IOException e) {
      throw new RuntimeException(e); // this is impossible with
      // ByteArrayOutputStream; crash hard
      // if this happens
    }
    return new PasswordToken(Base64.encodeBase64(bytes.toByteArray()));
  }
  
  private static byte[] getSystemConfigChecksum() throws NoSuchAlgorithmException {
    if (confChecksum == null) {
      MessageDigest md = MessageDigest.getInstance(Constants.PW_HASH_ALGORITHM);
      
      // seed the config with the version and instance id, so at least
      // it's not empty
      md.update(Constants.WIRE_VERSION.toString().getBytes(Constants.UTF8));
      md.update(HdfsZooInstance.getInstance().getInstanceID().getBytes(Constants.UTF8));
      
      for (Entry<String,String> entry : ServerConfiguration.getSiteConfiguration()) {
        // only include instance properties
        if (entry.getKey().startsWith(Property.INSTANCE_PREFIX.toString())) {
          md.update(entry.getKey().getBytes(Constants.UTF8));
          md.update(entry.getValue().getBytes(Constants.UTF8));
        }
      }
      
      confChecksum = md.digest();
    }
    return confChecksum;
  }
}
