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
package org.apache.accumulo.server.grpc;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.rpc.grpc.GrpcUtil;
import org.apache.accumulo.grpc.compaction.protobuf.CompactionCoordinatorServiceGrpc;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

/**
 * Simple wrapper to start/stop the grpc server
 */
public class CompactionCoordinatorServiceServer {

  private static final Logger logger =
      LoggerFactory.getLogger(CompactionCoordinatorServiceServer.class);

  private final int port;
  private final Server server;

  public CompactionCoordinatorServiceServer(
      CompactionCoordinatorServiceGrpc.CompactionCoordinatorServiceImplBase service, int port)
      throws IOException {
    this(Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create()), service, port);
  }

  public CompactionCoordinatorServiceServer(ServerBuilder<?> serverBuilder,
      CompactionCoordinatorServiceGrpc.CompactionCoordinatorServiceImplBase service, int port) {
    this.port = port;
    this.server = serverBuilder.addService(service).build();
  }

  public CompactionCoordinatorServiceServer(ServerContext context,
      CompactionCoordinatorServiceGrpc.CompactionCoordinatorServiceImplBase service, int port) {
    this.port = port;
    var serverBuilder = NettyServerBuilder.forPort(port);

    Optional.ofNullable(context.getServerSslParams()).ifPresent(
        sslParams -> serverBuilder.sslContext(GrpcUtil.buildSslContext(sslParams, true)));

    this.server = serverBuilder.addService(service).build();
  }

  public void start() throws IOException {
    server.start();
    logger.info("Starting CompactionCoordinatorService, listening on {}", port);
  }

  public void stop() {
    logger.info("Stopping CompactionCoordinatorService");
    try {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // TODO Do we care or want to handle this? It's on shutdown so probably can just log
      logger.debug(e.getMessage(), e);
    }
  }

}
