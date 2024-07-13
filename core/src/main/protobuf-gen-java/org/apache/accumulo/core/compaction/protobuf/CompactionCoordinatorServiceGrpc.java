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

  private static volatile io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.CompactionCompletedRequest,
      com.google.protobuf.Empty> getCompactionCompletedMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CompactionCompleted",
      requestType = org.apache.accumulo.core.compaction.protobuf.CompactionCompletedRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.CompactionCompletedRequest,
      com.google.protobuf.Empty> getCompactionCompletedMethod() {
    io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.CompactionCompletedRequest, com.google.protobuf.Empty> getCompactionCompletedMethod;
    if ((getCompactionCompletedMethod = CompactionCoordinatorServiceGrpc.getCompactionCompletedMethod) == null) {
      synchronized (CompactionCoordinatorServiceGrpc.class) {
        if ((getCompactionCompletedMethod = CompactionCoordinatorServiceGrpc.getCompactionCompletedMethod) == null) {
          CompactionCoordinatorServiceGrpc.getCompactionCompletedMethod = getCompactionCompletedMethod =
              io.grpc.MethodDescriptor.<org.apache.accumulo.core.compaction.protobuf.CompactionCompletedRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CompactionCompleted"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.accumulo.core.compaction.protobuf.CompactionCompletedRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new CompactionCoordinatorServiceMethodDescriptorSupplier("CompactionCompleted"))
              .build();
        }
      }
    }
    return getCompactionCompletedMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.UpdateCompactionStatusRequest,
      com.google.protobuf.Empty> getUpdateCompactionStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateCompactionStatus",
      requestType = org.apache.accumulo.core.compaction.protobuf.UpdateCompactionStatusRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.UpdateCompactionStatusRequest,
      com.google.protobuf.Empty> getUpdateCompactionStatusMethod() {
    io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.UpdateCompactionStatusRequest, com.google.protobuf.Empty> getUpdateCompactionStatusMethod;
    if ((getUpdateCompactionStatusMethod = CompactionCoordinatorServiceGrpc.getUpdateCompactionStatusMethod) == null) {
      synchronized (CompactionCoordinatorServiceGrpc.class) {
        if ((getUpdateCompactionStatusMethod = CompactionCoordinatorServiceGrpc.getUpdateCompactionStatusMethod) == null) {
          CompactionCoordinatorServiceGrpc.getUpdateCompactionStatusMethod = getUpdateCompactionStatusMethod =
              io.grpc.MethodDescriptor.<org.apache.accumulo.core.compaction.protobuf.UpdateCompactionStatusRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateCompactionStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.accumulo.core.compaction.protobuf.UpdateCompactionStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new CompactionCoordinatorServiceMethodDescriptorSupplier("UpdateCompactionStatus"))
              .build();
        }
      }
    }
    return getUpdateCompactionStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.CompactionFailedRequest,
      com.google.protobuf.Empty> getCompactionFailedMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CompactionFailed",
      requestType = org.apache.accumulo.core.compaction.protobuf.CompactionFailedRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.CompactionFailedRequest,
      com.google.protobuf.Empty> getCompactionFailedMethod() {
    io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.CompactionFailedRequest, com.google.protobuf.Empty> getCompactionFailedMethod;
    if ((getCompactionFailedMethod = CompactionCoordinatorServiceGrpc.getCompactionFailedMethod) == null) {
      synchronized (CompactionCoordinatorServiceGrpc.class) {
        if ((getCompactionFailedMethod = CompactionCoordinatorServiceGrpc.getCompactionFailedMethod) == null) {
          CompactionCoordinatorServiceGrpc.getCompactionFailedMethod = getCompactionFailedMethod =
              io.grpc.MethodDescriptor.<org.apache.accumulo.core.compaction.protobuf.CompactionFailedRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CompactionFailed"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.accumulo.core.compaction.protobuf.CompactionFailedRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new CompactionCoordinatorServiceMethodDescriptorSupplier("CompactionFailed"))
              .build();
        }
      }
    }
    return getCompactionFailedMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.GetRunningCompactionsRequest,
      org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList> getGetRunningCompactionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetRunningCompactions",
      requestType = org.apache.accumulo.core.compaction.protobuf.GetRunningCompactionsRequest.class,
      responseType = org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.GetRunningCompactionsRequest,
      org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList> getGetRunningCompactionsMethod() {
    io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.GetRunningCompactionsRequest, org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList> getGetRunningCompactionsMethod;
    if ((getGetRunningCompactionsMethod = CompactionCoordinatorServiceGrpc.getGetRunningCompactionsMethod) == null) {
      synchronized (CompactionCoordinatorServiceGrpc.class) {
        if ((getGetRunningCompactionsMethod = CompactionCoordinatorServiceGrpc.getGetRunningCompactionsMethod) == null) {
          CompactionCoordinatorServiceGrpc.getGetRunningCompactionsMethod = getGetRunningCompactionsMethod =
              io.grpc.MethodDescriptor.<org.apache.accumulo.core.compaction.protobuf.GetRunningCompactionsRequest, org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetRunningCompactions"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.accumulo.core.compaction.protobuf.GetRunningCompactionsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList.getDefaultInstance()))
              .setSchemaDescriptor(new CompactionCoordinatorServiceMethodDescriptorSupplier("GetRunningCompactions"))
              .build();
        }
      }
    }
    return getGetRunningCompactionsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.GetCompletedCompactionsRequest,
      org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList> getGetCompletedCompactionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCompletedCompactions",
      requestType = org.apache.accumulo.core.compaction.protobuf.GetCompletedCompactionsRequest.class,
      responseType = org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.GetCompletedCompactionsRequest,
      org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList> getGetCompletedCompactionsMethod() {
    io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.GetCompletedCompactionsRequest, org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList> getGetCompletedCompactionsMethod;
    if ((getGetCompletedCompactionsMethod = CompactionCoordinatorServiceGrpc.getGetCompletedCompactionsMethod) == null) {
      synchronized (CompactionCoordinatorServiceGrpc.class) {
        if ((getGetCompletedCompactionsMethod = CompactionCoordinatorServiceGrpc.getGetCompletedCompactionsMethod) == null) {
          CompactionCoordinatorServiceGrpc.getGetCompletedCompactionsMethod = getGetCompletedCompactionsMethod =
              io.grpc.MethodDescriptor.<org.apache.accumulo.core.compaction.protobuf.GetCompletedCompactionsRequest, org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCompletedCompactions"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.accumulo.core.compaction.protobuf.GetCompletedCompactionsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList.getDefaultInstance()))
              .setSchemaDescriptor(new CompactionCoordinatorServiceMethodDescriptorSupplier("GetCompletedCompactions"))
              .build();
        }
      }
    }
    return getGetCompletedCompactionsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.CancelRequest,
      com.google.protobuf.Empty> getCancelMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Cancel",
      requestType = org.apache.accumulo.core.compaction.protobuf.CancelRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.CancelRequest,
      com.google.protobuf.Empty> getCancelMethod() {
    io.grpc.MethodDescriptor<org.apache.accumulo.core.compaction.protobuf.CancelRequest, com.google.protobuf.Empty> getCancelMethod;
    if ((getCancelMethod = CompactionCoordinatorServiceGrpc.getCancelMethod) == null) {
      synchronized (CompactionCoordinatorServiceGrpc.class) {
        if ((getCancelMethod = CompactionCoordinatorServiceGrpc.getCancelMethod) == null) {
          CompactionCoordinatorServiceGrpc.getCancelMethod = getCancelMethod =
              io.grpc.MethodDescriptor.<org.apache.accumulo.core.compaction.protobuf.CancelRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Cancel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.accumulo.core.compaction.protobuf.CancelRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new CompactionCoordinatorServiceMethodDescriptorSupplier("Cancel"))
              .build();
        }
      }
    }
    return getCancelMethod;
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
     * Called by Compactor to get the next compaction job
     * </pre>
     */
    default void getCompactionJob(org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest request,
        io.grpc.stub.StreamObserver<org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCompactionJobMethod(), responseObserver);
    }

    /**
     * <pre>
     * Called by Compactor on successful completion of compaction job
     * </pre>
     */
    default void compactionCompleted(org.apache.accumulo.core.compaction.protobuf.CompactionCompletedRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCompactionCompletedMethod(), responseObserver);
    }

    /**
     * <pre>
     * Called by Compactor to update the Coordinator with the state of the compaction
     * </pre>
     */
    default void updateCompactionStatus(org.apache.accumulo.core.compaction.protobuf.UpdateCompactionStatusRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateCompactionStatusMethod(), responseObserver);
    }

    /**
     * <pre>
     * Called by Compactor on unsuccessful completion of compaction job
     * </pre>
     */
    default void compactionFailed(org.apache.accumulo.core.compaction.protobuf.CompactionFailedRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCompactionFailedMethod(), responseObserver);
    }

    /**
     * <pre>
     * Called by the Monitor to get progress information
     * </pre>
     */
    default void getRunningCompactions(org.apache.accumulo.core.compaction.protobuf.GetRunningCompactionsRequest request,
        io.grpc.stub.StreamObserver<org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetRunningCompactionsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Called by the Monitor to get progress information
     * </pre>
     */
    default void getCompletedCompactions(org.apache.accumulo.core.compaction.protobuf.GetCompletedCompactionsRequest request,
        io.grpc.stub.StreamObserver<org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCompletedCompactionsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Called by Compactor on unsuccessful completion of compaction job
     * </pre>
     */
    default void cancel(org.apache.accumulo.core.compaction.protobuf.CancelRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCancelMethod(), responseObserver);
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
     * Called by Compactor to get the next compaction job
     * </pre>
     */
    public void getCompactionJob(org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest request,
        io.grpc.stub.StreamObserver<org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCompactionJobMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Called by Compactor on successful completion of compaction job
     * </pre>
     */
    public void compactionCompleted(org.apache.accumulo.core.compaction.protobuf.CompactionCompletedRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCompactionCompletedMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Called by Compactor to update the Coordinator with the state of the compaction
     * </pre>
     */
    public void updateCompactionStatus(org.apache.accumulo.core.compaction.protobuf.UpdateCompactionStatusRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateCompactionStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Called by Compactor on unsuccessful completion of compaction job
     * </pre>
     */
    public void compactionFailed(org.apache.accumulo.core.compaction.protobuf.CompactionFailedRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCompactionFailedMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Called by the Monitor to get progress information
     * </pre>
     */
    public void getRunningCompactions(org.apache.accumulo.core.compaction.protobuf.GetRunningCompactionsRequest request,
        io.grpc.stub.StreamObserver<org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetRunningCompactionsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Called by the Monitor to get progress information
     * </pre>
     */
    public void getCompletedCompactions(org.apache.accumulo.core.compaction.protobuf.GetCompletedCompactionsRequest request,
        io.grpc.stub.StreamObserver<org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCompletedCompactionsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Called by Compactor on unsuccessful completion of compaction job
     * </pre>
     */
    public void cancel(org.apache.accumulo.core.compaction.protobuf.CancelRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCancelMethod(), getCallOptions()), request, responseObserver);
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
     * Called by Compactor to get the next compaction job
     * </pre>
     */
    public org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob getCompactionJob(org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCompactionJobMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Called by Compactor on successful completion of compaction job
     * </pre>
     */
    public com.google.protobuf.Empty compactionCompleted(org.apache.accumulo.core.compaction.protobuf.CompactionCompletedRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCompactionCompletedMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Called by Compactor to update the Coordinator with the state of the compaction
     * </pre>
     */
    public com.google.protobuf.Empty updateCompactionStatus(org.apache.accumulo.core.compaction.protobuf.UpdateCompactionStatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateCompactionStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Called by Compactor on unsuccessful completion of compaction job
     * </pre>
     */
    public com.google.protobuf.Empty compactionFailed(org.apache.accumulo.core.compaction.protobuf.CompactionFailedRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCompactionFailedMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Called by the Monitor to get progress information
     * </pre>
     */
    public org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList getRunningCompactions(org.apache.accumulo.core.compaction.protobuf.GetRunningCompactionsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetRunningCompactionsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Called by the Monitor to get progress information
     * </pre>
     */
    public org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList getCompletedCompactions(org.apache.accumulo.core.compaction.protobuf.GetCompletedCompactionsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCompletedCompactionsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Called by Compactor on unsuccessful completion of compaction job
     * </pre>
     */
    public com.google.protobuf.Empty cancel(org.apache.accumulo.core.compaction.protobuf.CancelRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCancelMethod(), getCallOptions(), request);
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
     * Called by Compactor to get the next compaction job
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob> getCompactionJob(
        org.apache.accumulo.core.compaction.protobuf.CompactionJobRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCompactionJobMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Called by Compactor on successful completion of compaction job
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> compactionCompleted(
        org.apache.accumulo.core.compaction.protobuf.CompactionCompletedRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCompactionCompletedMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Called by Compactor to update the Coordinator with the state of the compaction
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> updateCompactionStatus(
        org.apache.accumulo.core.compaction.protobuf.UpdateCompactionStatusRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateCompactionStatusMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Called by Compactor on unsuccessful completion of compaction job
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> compactionFailed(
        org.apache.accumulo.core.compaction.protobuf.CompactionFailedRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCompactionFailedMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Called by the Monitor to get progress information
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList> getRunningCompactions(
        org.apache.accumulo.core.compaction.protobuf.GetRunningCompactionsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetRunningCompactionsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Called by the Monitor to get progress information
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList> getCompletedCompactions(
        org.apache.accumulo.core.compaction.protobuf.GetCompletedCompactionsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCompletedCompactionsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Called by Compactor on unsuccessful completion of compaction job
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> cancel(
        org.apache.accumulo.core.compaction.protobuf.CancelRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCancelMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_COMPACTION_JOB = 0;
  private static final int METHODID_COMPACTION_COMPLETED = 1;
  private static final int METHODID_UPDATE_COMPACTION_STATUS = 2;
  private static final int METHODID_COMPACTION_FAILED = 3;
  private static final int METHODID_GET_RUNNING_COMPACTIONS = 4;
  private static final int METHODID_GET_COMPLETED_COMPACTIONS = 5;
  private static final int METHODID_CANCEL = 6;

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
        case METHODID_COMPACTION_COMPLETED:
          serviceImpl.compactionCompleted((org.apache.accumulo.core.compaction.protobuf.CompactionCompletedRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_UPDATE_COMPACTION_STATUS:
          serviceImpl.updateCompactionStatus((org.apache.accumulo.core.compaction.protobuf.UpdateCompactionStatusRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_COMPACTION_FAILED:
          serviceImpl.compactionFailed((org.apache.accumulo.core.compaction.protobuf.CompactionFailedRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_GET_RUNNING_COMPACTIONS:
          serviceImpl.getRunningCompactions((org.apache.accumulo.core.compaction.protobuf.GetRunningCompactionsRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList>) responseObserver);
          break;
        case METHODID_GET_COMPLETED_COMPACTIONS:
          serviceImpl.getCompletedCompactions((org.apache.accumulo.core.compaction.protobuf.GetCompletedCompactionsRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList>) responseObserver);
          break;
        case METHODID_CANCEL:
          serviceImpl.cancel((org.apache.accumulo.core.compaction.protobuf.CancelRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
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
        .addMethod(
          getCompactionCompletedMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.accumulo.core.compaction.protobuf.CompactionCompletedRequest,
              com.google.protobuf.Empty>(
                service, METHODID_COMPACTION_COMPLETED)))
        .addMethod(
          getUpdateCompactionStatusMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.accumulo.core.compaction.protobuf.UpdateCompactionStatusRequest,
              com.google.protobuf.Empty>(
                service, METHODID_UPDATE_COMPACTION_STATUS)))
        .addMethod(
          getCompactionFailedMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.accumulo.core.compaction.protobuf.CompactionFailedRequest,
              com.google.protobuf.Empty>(
                service, METHODID_COMPACTION_FAILED)))
        .addMethod(
          getGetRunningCompactionsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.accumulo.core.compaction.protobuf.GetRunningCompactionsRequest,
              org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList>(
                service, METHODID_GET_RUNNING_COMPACTIONS)))
        .addMethod(
          getGetCompletedCompactionsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.accumulo.core.compaction.protobuf.GetCompletedCompactionsRequest,
              org.apache.accumulo.core.compaction.protobuf.PExternalCompactionList>(
                service, METHODID_GET_COMPLETED_COMPACTIONS)))
        .addMethod(
          getCancelMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.apache.accumulo.core.compaction.protobuf.CancelRequest,
              com.google.protobuf.Empty>(
                service, METHODID_CANCEL)))
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
              .addMethod(getCompactionCompletedMethod())
              .addMethod(getUpdateCompactionStatusMethod())
              .addMethod(getCompactionFailedMethod())
              .addMethod(getGetRunningCompactionsMethod())
              .addMethod(getGetCompletedCompactionsMethod())
              .addMethod(getCancelMethod())
              .build();
        }
      }
    }
    return result;
  }
}
