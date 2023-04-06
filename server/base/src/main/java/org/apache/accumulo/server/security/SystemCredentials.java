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
package org.apache.accumulo.server.security;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.commons.codec.digest.Crypt;
import org.apache.hadoop.io.Writable;

/**
 * Credentials for the system services.
 *
 * @since 1.6.0
 */
public final class SystemCredentials extends Credentials {

  private static final String SYSTEM_PRINCIPAL = "!SYSTEM";

  private final TCredentials AS_THRIFT;

  public SystemCredentials(InstanceId instanceID, String principal, AuthenticationToken token) {
    super(principal, token);
    AS_THRIFT = super.toThrift(instanceID);
  }

  public static SystemCredentials get(InstanceId instanceID, SiteConfiguration siteConfig) {
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
    return new SystemCredentials(instanceID, principal,
        SystemToken.generate(instanceID, siteConfig));
  }

  @Override
  public TCredentials toThrift(InstanceId instanceID) {
    if (!AS_THRIFT.getInstanceId().equals(instanceID.canonical())) {
      throw new IllegalArgumentException("Unexpected instance used for "
          + SystemCredentials.class.getSimpleName() + ": " + instanceID);
    }
    return AS_THRIFT;
  }

  /**
   * An {@link AuthenticationToken} type for Accumulo servers for inter-server communication.
   *
   * @since 1.6.0
   */
  public static final class SystemToken extends PasswordToken {

    /**
     * Accumulo servers will only communicate with each other when this is the same.
     *
     * <ul>
     * <li>Initial version 2 for 1.5 to support rolling upgrades for bugfix releases (ACCUMULO-751)
     * <li>Bumped to 3 for 1.6 for additional namespace RPC changes (ACCUMULO-802)
     * <li>Bumped to 4 for 2.0 to prevent 1.9 and 2.0 servers from communicating (#1139)
     * <li>Bumped to 5 for 2.1 because of system credential hash incompatibility (#1798 / #1810)
     * </ul>
     */
    static final int INTERNAL_WIRE_VERSION = 5;

    static final String SALT_PREFIX = "$6$"; // SHA-512
    private static final String SALT_SUFFIX = "$";

    /**
     * A Constructor for {@link Writable}.
     */
    public SystemToken() {}

    private SystemToken(byte[] systemPassword) {
      super(systemPassword);
    }

    private static String hashInstanceConfigs(InstanceId instanceID, SiteConfiguration siteConfig) {
      String wireVersion = Integer.toString(INTERNAL_WIRE_VERSION);
      // seed the config with the version and instance id, so at least it's not empty
      var sb = new StringBuilder(wireVersion).append("\t").append(instanceID).append("\t");
      siteConfig.forEach(entry -> {
        String k = entry.getKey();
        String v = entry.getValue();
        // only include instance properties
        if (k.startsWith(Property.INSTANCE_PREFIX.toString())) {
          sb.append(k).append("=").append(v).append("\t");
        }
      });
      return Crypt.crypt(sb.toString(), SALT_PREFIX + wireVersion + SALT_SUFFIX);
    }

    private static SystemToken generate(InstanceId instanceID, SiteConfiguration siteConfig) {
      byte[] instanceIdBytes = instanceID.canonical().getBytes(UTF_8);
      byte[] configHash = hashInstanceConfigs(instanceID, siteConfig).getBytes(UTF_8);

      // the actual token is a base64-encoded composition of:
      // 1. the wire version,
      // 2. the instance ID, and
      // 3. a hash of the subset of config properties starting with 'instance.'
      int wireVersion = INTERNAL_WIRE_VERSION;
      int capacity = 3 * (Integer.SIZE / Byte.SIZE) + instanceIdBytes.length + configHash.length;
      try (var bytes = new ByteArrayOutputStream(capacity); var out = new DataOutputStream(bytes)) {
        out.write(wireVersion * -1);
        out.write(instanceIdBytes.length);
        out.write(instanceIdBytes);
        out.write(configHash.length);
        out.write(configHash);
        return new SystemToken(Base64.getEncoder().encode(bytes.toByteArray()));
      } catch (IOException e) {
        // this is impossible with ByteArrayOutputStream; crash hard if this happens
        throw new AssertionError("byte array output stream somehow did the impossible", e);
      }
    }
  }

}
