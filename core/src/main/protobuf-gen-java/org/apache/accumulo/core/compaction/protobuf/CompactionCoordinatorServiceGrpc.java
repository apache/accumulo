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
package org.apache.accumulo.core.compaction.protobuf;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Interface exported by the server.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.64.0)",
    comments = "Source: compaction-coordinator.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class CompactionCoordinatorServiceGrpc {

  private CompactionCoordinatorServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "compaction_coordinator.CompactionCoordinatorService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest,
      org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob> getGetCompactionJobMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCompactionJob",
      requestType = org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest.class,
      responseType = org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest,
      org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob> getGetCompactionJobMethod() {
    io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest, org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob> getGetCompactionJobMethod;
    if ((getGetCompactionJobMethod = CompactionCoordinatorServiceGrpc.getGetCompactionJobMethod) == null) {
      synchronized (CompactionCoordinatorServiceGrpc.class) {
        if ((getGetCompactionJobMethod = CompactionCoordinatorServiceGrpc.getGetCompactionJobMethod) == null) {
          CompactionCoordinatorServiceGrpc.getGetCompactionJobMethod = getGetCompactionJobMethod =
              io.grpc.MethodDescriptor.<org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest, org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCompactionJob"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob.getDefaultInstance()))
              .setSchemaDescriptor(new CompactionCoordinatorServiceMethodDescriptorSupplier("GetCompactionJob"))
              .build();
        }
      }
    }
    return getGetCompactionJobMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CompactionCoordinatorServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CompactionCoordinatorServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CompactionCoordinatorServiceStub>() {
        @java.lang.Override
        public CompactionCoordinatorServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CompactionCoordinatorServiceStub(channel, callOptions);
        }
      };
    return CompactionCoordinatorServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CompactionCoordinatorServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CompactionCoordinatorServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CompactionCoordinatorServiceBlockingStub>() {
        @java.lang.Override
        public CompactionCoordinatorServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CompactionCoordinatorServiceBlockingStub(channel, callOptions);
        }
      };
    return CompactionCoordinatorServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CompactionCoordinatorServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CompactionCoordinatorServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CompactionCoordinatorServiceFutureStub>() {
        @java.lang.Override
        public CompactionCoordinatorServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CompactionCoordinatorServiceFutureStub(channel, callOptions);
        }
      };
    return CompactionCoordinatorServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * A simple RPC.
     * Obtains the feature at a given position.
     * A feature with an empty name is returned if there's no feature at the given
     * position.
     * </pre>
     */
    default void getCompactionJob(org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest request,
        io.grpc.stub.StreamObserver<org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCompactionJobMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service CompactionCoordinatorService.
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public static abstract class CompactionCoordinatorServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return CompactionCoordinatorServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service CompactionCoordinatorService.
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public static final class CompactionCoordinatorServiceStub
      extends io.grpc.stub.AbstractAsyncStub<CompactionCoordinatorServiceStub> {
    private CompactionCoordinatorServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CompactionCoordinatorServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CompactionCoordinatorServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * A simple RPC.
     * Obtains the feature at a given position.
     * A feature with an empty name is returned if there's no feature at the given
     * position.
     * </pre>
     */
    public void getCompactionJob(org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest request,
        io.grpc.stub.StreamObserver<org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCompactionJobMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service CompactionCoordinatorService.
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public static final class CompactionCoordinatorServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<CompactionCoordinatorServiceBlockingStub> {
    private CompactionCoordinatorServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CompactionCoordinatorServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CompactionCoordinatorServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * A simple RPC.
     * Obtains the feature at a given position.
     * A feature with an empty name is returned if there's no feature at the given
     * position.
     * </pre>
     */
    public org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob getCompactionJob(org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCompactionJobMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service CompactionCoordinatorService.
   * <pre>
   * Interface exported by the server.
   * </pre>
   */
  public static final class CompactionCoordinatorServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<CompactionCoordinatorServiceFutureStub> {
    private CompactionCoordinatorServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CompactionCoordinatorServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CompactionCoordinatorServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * A simple RPC.
     * Obtains the feature at a given position.
     * A feature with an empty name is returned if there's no feature at the given
     * position.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob> getCompactionJob(
        org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCompactionJobMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_COMPACTION_JOB = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_COMPACTION_JOB:
          serviceImpl.getCompactionJob((org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getGetCompactionJobMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest,
              org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob>(
                service, METHODID_GET_COMPACTION_JOB)))
        .build();
  }

  private static abstract class CompactionCoordinatorServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CompactionCoordinatorServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.accumulo.core.compaction.protobuf.CompactionCoordinatorServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CompactionCoordinatorService");
    }
  }

  private static final class CompactionCoordinatorServiceFileDescriptorSupplier
      extends CompactionCoordinatorServiceBaseDescriptorSupplier {
    CompactionCoordinatorServiceFileDescriptorSupplier() {}
  }

  private static final class CompactionCoordinatorServiceMethodDescriptorSupplier
      extends CompactionCoordinatorServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    CompactionCoordinatorServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (CompactionCoordinatorServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CompactionCoordinatorServiceFileDescriptorSupplier())
              .addMethod(getGetCompactionJobMethod())
              .build();
        }
      }
    }
    return result;
  }
}
