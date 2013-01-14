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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecurityPermission;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.master.state.TabletServerState;
import org.apache.commons.codec.binary.Base64;

public class SecurityConstants {
  private static SecurityPermission SYSTEM_CREDENTIALS_PERMISSION = new SecurityPermission("systemCredentialsPermission");
  
  public static final String SYSTEM_USERNAME = "!SYSTEM";
  private static final byte[] SYSTEM_PASSWORD = makeSystemPassword();
  private static final AuthInfo systemCredentials = new AuthInfo(SYSTEM_USERNAME, ByteBuffer.wrap(SYSTEM_PASSWORD), HdfsZooInstance.getInstance()
      .getInstanceID());
  public static byte[] confChecksum = null;
  
  public static AuthInfo getSystemCredentials() {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(SYSTEM_CREDENTIALS_PERMISSION);
    }
    return systemCredentials;
  }
  
  private static byte[] makeSystemPassword() {
    byte[] version = Constants.VERSION.getBytes();
    byte[] inst = HdfsZooInstance.getInstance().getInstanceID().getBytes();
    try {
      confChecksum = getSystemConfigChecksum();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Failed to compute configuration checksum", e);
    }
    
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(3 * (Integer.SIZE / Byte.SIZE) + version.length + inst.length + confChecksum.length);
    DataOutputStream out = new DataOutputStream(bytes);
    try {
      out.write(version.length);
      out.write(version);
      out.write(inst.length);
      out.write(inst);
      out.write(confChecksum.length);
      out.write(confChecksum);
    } catch (IOException e) {
      throw new RuntimeException(e); // this is impossible with
      // ByteArrayOutputStream; crash hard
      // if this happens
    }
    return Base64.encodeBase64(bytes.toByteArray());
  }
  
  /**
   * Compare a byte array to the system password.
   * 
   * @return RESERVED if the passwords match, otherwise a state that describes the failure state
   */
  public static TabletServerState compareSystemPassword(byte[] base64encodedPassword) {
    if (Arrays.equals(SYSTEM_PASSWORD, base64encodedPassword))
      return TabletServerState.RESERVED;
    
    // parse to determine why
    byte[] decodedPassword = Base64.decodeBase64(base64encodedPassword);
    boolean versionFails, instanceFails, confFails;
    
    ByteArrayInputStream bytes = new ByteArrayInputStream(decodedPassword);
    DataInputStream in = new DataInputStream(bytes);
    try {
      byte[] buff = new byte[in.readInt()];
      in.readFully(buff);
      versionFails = !Arrays.equals(buff, Constants.VERSION.getBytes());
      buff = new byte[in.readInt()];
      in.readFully(buff);
      instanceFails = !Arrays.equals(buff, HdfsZooInstance.getInstance().getInstanceID().getBytes());
      buff = new byte[in.readInt()];
      in.readFully(buff);
      confFails = !Arrays.equals(buff, getSystemConfigChecksum());
      if (in.available() > 0)
        throw new IOException();
    } catch (IOException e) {
      return TabletServerState.BAD_SYSTEM_PASSWORD;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Failed to compare system password", e);
    }
    
    // to be fair, I wanted to do this as one big return statement with
    // nested ternary conditionals, but
    // this is more readable; no fun :/
    if (versionFails) {
      if (instanceFails)
        return confFails ? TabletServerState.BAD_VERSION_AND_INSTANCE_AND_CONFIG : TabletServerState.BAD_VERSION_AND_INSTANCE;
      return confFails ? TabletServerState.BAD_VERSION_AND_CONFIG : TabletServerState.BAD_VERSION;
    }
    if (instanceFails)
      return confFails ? TabletServerState.BAD_INSTANCE_AND_CONFIG : TabletServerState.BAD_INSTANCE;
    return confFails ? TabletServerState.BAD_CONFIG : TabletServerState.BAD_SYSTEM_PASSWORD;
  }
  
  private static byte[] getSystemConfigChecksum() throws NoSuchAlgorithmException {
    if (confChecksum == null) {
      MessageDigest md = MessageDigest.getInstance(Constants.PW_HASH_ALGORITHM);
      
      // seed the config with the version and instance id, so at least
      // it's not empty
      md.update(Constants.VERSION.getBytes());
      md.update(HdfsZooInstance.getInstance().getInstanceID().getBytes());
      
      for (Entry<String,String> entry : ServerConfiguration.getSiteConfiguration()) {
        // only include instance properties
        if (entry.getKey().startsWith(Property.INSTANCE_PREFIX.toString())) {
          md.update(entry.getKey().getBytes());
          md.update(entry.getValue().getBytes());
        }
      }
      
      confChecksum = md.digest();
    }
    return confChecksum;
  }
}
