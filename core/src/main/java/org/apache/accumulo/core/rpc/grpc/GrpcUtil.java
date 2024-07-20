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

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.net.HostAndPort;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

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
    // TODO: we are just using the config for now for the Port as we need to update ZK to
    // store the new gRPC port for the service as currently this is just the Thrift port
    return grpcChannels.get(hostAndPort,
        hp -> ManagedChannelBuilder
            .forAddress(hp.getHost(),
                context.getConfiguration().getPortStream(Property.MANAGER_GRPC_CLIENTPORT)
                    .findFirst().orElseThrow())
            .idleTimeout(context.getClientTimeoutInMillis(), TimeUnit.MILLISECONDS).usePlaintext()
            .build());
  }

}
