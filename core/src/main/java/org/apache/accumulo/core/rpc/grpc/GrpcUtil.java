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
package org.apache.accumulo.core.rpc.grpc;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SslConnectionParams;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettySslContextChannelCredentials;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;

public class GrpcUtil {

  // TODO: We are just sharing a single ManagedChannel for all RPC requests to the same server
  // We need to look into pooling to see if that is necessary with gRPC or if ManagedChannel can
  // handle multiple connections using ManagedChannelBuilder or NettyChannelBuilder
  private static final Cache<HostAndPort,ManagedChannel> grpcChannels = Caffeine.newBuilder()
      // TODO: When to expire?
      .expireAfterAccess(60_000, TimeUnit.MILLISECONDS)
      .removalListener((RemovalListener<HostAndPort,ManagedChannel>) (key, value, cause) -> {
        if (value != null) {
          value.shutdownNow();
        }
      }).build();

  public static ManagedChannel getChannel(HostAndPort hostAndPort, ClientContext context) {
    ChannelCredentials credentials =
        Optional.ofNullable(SslConnectionParams.forClient(context.getConfiguration()))
            .map(sslParams -> NettySslContextChannelCredentials
                .create(GrpcUtil.buildSslContext(sslParams, false)))
            .orElse(InsecureChannelCredentials.create());

    // TODO: we are just using the config for now for the Port as we need to update ZK to
    // store the new gRPC port for the service as currently this is just the Thrift port
    return grpcChannels.get(hostAndPort,
        hp -> Grpc
            .newChannelBuilderForAddress(hp.getHost(),
                context.getConfiguration().getPortStream(Property.MANAGER_GRPC_CLIENTPORT)
                    .findFirst().orElseThrow(),
                credentials)
            .idleTimeout(context.getClientTimeoutInMillis(), TimeUnit.MILLISECONDS).build());
  }

  // TODO: This builds the SSL context but still needs to be tested that everything works
  // for client and server
  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who providing the keystore file")
  public static SslContext buildSslContext(SslConnectionParams sslParams, boolean server) {
    Preconditions.checkArgument(!server || sslParams.isKeyStoreSet(),
        "KeyStore must be set on the server.");

    try {
      KeyManagerFactory kmf = null;
      if (sslParams.isKeyStoreSet()) {
        KeyStore keyStore = KeyStore.getInstance(sslParams.getKeyStoreType());
        try (FileInputStream fis = new FileInputStream(sslParams.getKeyStorePath())) {
          keyStore.load(fis, sslParams.getKeyStorePass().toCharArray());
        }
        kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, sslParams.getKeyStorePass().toCharArray());
      }

      final var sslContextBuilder =
          server ? SslContextBuilder.forServer(kmf) : SslContextBuilder.forClient();
      sslContextBuilder.protocols(sslParams.getServerProtocols());

      if (sslParams.isTrustStoreSet()) {
        KeyStore trustStore = KeyStore.getInstance(sslParams.getTrustStoreType());
        try (FileInputStream fis = new FileInputStream(sslParams.getTrustStorePath())) {
          trustStore.load(fis, sslParams.getTrustStorePass().toCharArray());
        }
        var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        sslContextBuilder.trustManager(tmf);
      }

      if (sslParams.isClientAuth()) {
        sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
      }

      return sslContextBuilder.build();
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }
  }
}
