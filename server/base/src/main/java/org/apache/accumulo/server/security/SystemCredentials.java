/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.security;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.hadoop.io.Writable;

/**
 * Credentials for the system services.
 *
 * @since 1.6.0
 */
public final class SystemCredentials extends Credentials {

  private static final String SYSTEM_PRINCIPAL = "!SYSTEM";

  private final TCredentials AS_THRIFT;

  public SystemCredentials(String instanceID, String principal, AuthenticationToken token) {
    super(principal, token);
    AS_THRIFT = super.toThrift(instanceID);
  }

  public static SystemCredentials get(String instanceID, SiteConfiguration siteConfig) {
    String principal = SYSTEM_PRINCIPAL;
    if (siteConfig.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      // Use the server's kerberos principal as the Accumulo principal. We could also unwrap the
      // principal server-side, but the principal for SystemCredentials
      // isn't actually used anywhere, so it really doesn't matter. We can't include the kerberos
      // principal in the SystemToken as it would break equality when
      // different Accumulo servers are using different kerberos principals are their accumulo
      // principal
      principal =
          SecurityUtil.getServerPrincipal(siteConfig.get(Property.GENERAL_KERBEROS_PRINCIPAL));
    }
    return new SystemCredentials(instanceID, principal, SystemToken.get(instanceID, siteConfig));
  }

  @Override
  public TCredentials toThrift(String instanceID) {
    if (!AS_THRIFT.getInstanceId().equals(instanceID))
      throw new IllegalArgumentException("Unexpected instance used for "
          + SystemCredentials.class.getSimpleName() + ": " + instanceID);
    return AS_THRIFT;
  }

  /**
   * An {@link AuthenticationToken} type for Accumulo servers for inter-server communication.
   *
   * @since 1.6.0
   */
  public static final class SystemToken extends PasswordToken {

    /**
     * Accumulo servers will only communicate with each other when this is the same. Bumped for 2.0
     * to prevent 1.9 and 2.0 servers from communicating.
     */
    private static final Integer INTERNAL_WIRE_VERSION = 4;

    /**
     * A Constructor for {@link Writable}.
     */
    public SystemToken() {}

    private SystemToken(byte[] systemPassword) {
      super(systemPassword);
    }

    private static SystemToken get(String instanceID, SiteConfiguration siteConfig) {
      byte[] instanceIdBytes = instanceID.getBytes(UTF_8);
      byte[] confChecksum;
      MessageDigest md;
      try {
        String hashAlgorithm = siteConfig.get(Property.INSTANCE_SYSTEM_TOKEN_HASH_TYPE);
        md = MessageDigest.getInstance(hashAlgorithm);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("Failed to compute configuration checksum", e);
      }

      // seed the config with the version and instance id, so at least it's not empty
      md.update(INTERNAL_WIRE_VERSION.toString().getBytes(UTF_8));
      md.update(instanceIdBytes);

      for (Entry<String,String> entry : siteConfig) {
        // only include instance properties
        if (entry.getKey().startsWith(Property.INSTANCE_PREFIX.toString())) {
          md.update(entry.getKey().getBytes(UTF_8));
          md.update(entry.getValue().getBytes(UTF_8));
        }
      }
      confChecksum = md.digest();

      int wireVersion = INTERNAL_WIRE_VERSION;

      ByteArrayOutputStream bytes = new ByteArrayOutputStream(
          3 * (Integer.SIZE / Byte.SIZE) + instanceIdBytes.length + confChecksum.length);
      DataOutputStream out = new DataOutputStream(bytes);
      try {
        out.write(wireVersion * -1);
        out.write(instanceIdBytes.length);
        out.write(instanceIdBytes);
        out.write(confChecksum.length);
        out.write(confChecksum);
      } catch (IOException e) {
        // this is impossible with ByteArrayOutputStream; crash hard if this happens
        throw new RuntimeException(e);
      }
      return new SystemToken(Base64.getEncoder().encode(bytes.toByteArray()));
    }
  }

}
