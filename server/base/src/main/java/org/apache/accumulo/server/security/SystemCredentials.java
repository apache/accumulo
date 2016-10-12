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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.ServerConstants;
import org.apache.hadoop.io.Writable;

/**
 * Credentials for the system services.
 *
 * @since 1.6.0
 */
public final class SystemCredentials extends Credentials {

  private static final String SYSTEM_PRINCIPAL = "!SYSTEM";

  private final TCredentials AS_THRIFT;

  public SystemCredentials(Instance instance, String principal, AuthenticationToken token) {
    super(principal, token);
    AS_THRIFT = super.toThrift(instance);
  }

  public static SystemCredentials get(Instance instance) {
    String principal = SYSTEM_PRINCIPAL;
    AccumuloConfiguration conf = SiteConfiguration.getInstance();
    if (conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      // Use the server's kerberos principal as the Accumulo principal. We could also unwrap the principal server-side, but the principal for SystemCredentials
      // isnt' actually used anywhere, so it really doesn't matter. We can't include the kerberos principal in the SystemToken as it would break equality when
      // different Accumulo servers are using different kerberos principals are their accumulo principal
      principal = SecurityUtil.getServerPrincipal(conf.get(Property.GENERAL_KERBEROS_PRINCIPAL));
    }
    return new SystemCredentials(instance, principal, SystemToken.get(instance));
  }

  @Override
  public TCredentials toThrift(Instance instance) {
    if (!AS_THRIFT.getInstanceId().equals(instance.getInstanceID()))
      throw new IllegalArgumentException("Unexpected instance used for " + SystemCredentials.class.getSimpleName() + ": " + instance.getInstanceID());
    return AS_THRIFT;
  }

  /**
   * An {@link AuthenticationToken} type for Accumulo servers for inter-server communication.
   *
   * @since 1.6.0
   */
  public static final class SystemToken extends PasswordToken {

    /**
     * A Constructor for {@link Writable}.
     */
    public SystemToken() {}

    private SystemToken(byte[] systemPassword) {
      super(systemPassword);
    }

    private static SystemToken get(Instance instance) {
      byte[] instanceIdBytes = instance.getInstanceID().getBytes(UTF_8);
      byte[] confChecksum;
      MessageDigest md;
      try {
        md = MessageDigest.getInstance(Constants.PW_HASH_ALGORITHM);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("Failed to compute configuration checksum", e);
      }

      // seed the config with the version and instance id, so at least it's not empty
      md.update(ServerConstants.WIRE_VERSION.toString().getBytes(UTF_8));
      md.update(instanceIdBytes);

      for (Entry<String,String> entry : SiteConfiguration.getInstance()) {
        // only include instance properties
        if (entry.getKey().startsWith(Property.INSTANCE_PREFIX.toString())) {
          md.update(entry.getKey().getBytes(UTF_8));
          md.update(entry.getValue().getBytes(UTF_8));
        }
      }
      confChecksum = md.digest();

      int wireVersion = ServerConstants.WIRE_VERSION;

      ByteArrayOutputStream bytes = new ByteArrayOutputStream(3 * (Integer.SIZE / Byte.SIZE) + instanceIdBytes.length + confChecksum.length);
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
