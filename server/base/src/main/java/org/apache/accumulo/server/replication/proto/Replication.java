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
// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: src/main/protobuf/replication.proto

package org.apache.accumulo.server.replication.proto;

@SuppressWarnings({"unused"}) public final class Replication {
  private Replication() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface StatusOrBuilder extends
      // @@protoc_insertion_point(interface_extends:Status)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <pre>
     * offset where replication should start
     * </pre>
     *
     * <code>optional int64 begin = 1 [default = 0];</code>
     * @return Whether the begin field is set.
     */
    boolean hasBegin();
    /**
     * <pre>
     * offset where replication should start
     * </pre>
     *
     * <code>optional int64 begin = 1 [default = 0];</code>
     * @return The begin.
     */
    long getBegin();

    /**
     * <pre>
     * offset where data is ready for replication
     * </pre>
     *
     * <code>optional int64 end = 2 [default = 0];</code>
     * @return Whether the end field is set.
     */
    boolean hasEnd();
    /**
     * <pre>
     * offset where data is ready for replication
     * </pre>
     *
     * <code>optional int64 end = 2 [default = 0];</code>
     * @return The end.
     */
    long getEnd();

    /**
     * <pre>
     * do we have a discrete 'end'
     * </pre>
     *
     * <code>optional bool infiniteEnd = 3 [default = false];</code>
     * @return Whether the infiniteEnd field is set.
     */
    boolean hasInfiniteEnd();
    /**
     * <pre>
     * do we have a discrete 'end'
     * </pre>
     *
     * <code>optional bool infiniteEnd = 3 [default = false];</code>
     * @return The infiniteEnd.
     */
    boolean getInfiniteEnd();

    /**
     * <pre>
     * will more data be appended to the file
     * </pre>
     *
     * <code>optional bool closed = 4 [default = false];</code>
     * @return Whether the closed field is set.
     */
    boolean hasClosed();
    /**
     * <pre>
     * will more data be appended to the file
     * </pre>
     *
     * <code>optional bool closed = 4 [default = false];</code>
     * @return The closed.
     */
    boolean getClosed();

    /**
     * <pre>
     * when, in ms, was the file created?
     * </pre>
     *
     * <code>optional int64 createdTime = 5 [default = 0];</code>
     * @return Whether the createdTime field is set.
     */
    boolean hasCreatedTime();
    /**
     * <pre>
     * when, in ms, was the file created?
     * </pre>
     *
     * <code>optional int64 createdTime = 5 [default = 0];</code>
     * @return The createdTime.
     */
    long getCreatedTime();
  }
  /**
   * Protobuf type {@code Status}
   */
  public static final class Status extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:Status)
      StatusOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use Status.newBuilder() to construct.
    private Status(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private Status() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new Status();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private Status(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 8: {
              bitField0_ |= 0x00000001;
              begin_ = input.readInt64();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              end_ = input.readInt64();
              break;
            }
            case 24: {
              bitField0_ |= 0x00000004;
              infiniteEnd_ = input.readBool();
              break;
            }
            case 32: {
              bitField0_ |= 0x00000008;
              closed_ = input.readBool();
              break;
            }
            case 40: {
              bitField0_ |= 0x00000010;
              createdTime_ = input.readInt64();
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.accumulo.server.replication.proto.Replication.internal_static_Status_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.accumulo.server.replication.proto.Replication.internal_static_Status_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.accumulo.server.replication.proto.Replication.Status.class, org.apache.accumulo.server.replication.proto.Replication.Status.Builder.class);
    }

    private int bitField0_;
    public static final int BEGIN_FIELD_NUMBER = 1;
    private long begin_;
    /**
     * <pre>
     * offset where replication should start
     * </pre>
     *
     * <code>optional int64 begin = 1 [default = 0];</code>
     * @return Whether the begin field is set.
     */
    @java.lang.Override
    public boolean hasBegin() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <pre>
     * offset where replication should start
     * </pre>
     *
     * <code>optional int64 begin = 1 [default = 0];</code>
     * @return The begin.
     */
    @java.lang.Override
    public long getBegin() {
      return begin_;
    }

    public static final int END_FIELD_NUMBER = 2;
    private long end_;
    /**
     * <pre>
     * offset where data is ready for replication
     * </pre>
     *
     * <code>optional int64 end = 2 [default = 0];</code>
     * @return Whether the end field is set.
     */
    @java.lang.Override
    public boolean hasEnd() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <pre>
     * offset where data is ready for replication
     * </pre>
     *
     * <code>optional int64 end = 2 [default = 0];</code>
     * @return The end.
     */
    @java.lang.Override
    public long getEnd() {
      return end_;
    }

    public static final int INFINITEEND_FIELD_NUMBER = 3;
    private boolean infiniteEnd_;
    /**
     * <pre>
     * do we have a discrete 'end'
     * </pre>
     *
     * <code>optional bool infiniteEnd = 3 [default = false];</code>
     * @return Whether the infiniteEnd field is set.
     */
    @java.lang.Override
    public boolean hasInfiniteEnd() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <pre>
     * do we have a discrete 'end'
     * </pre>
     *
     * <code>optional bool infiniteEnd = 3 [default = false];</code>
     * @return The infiniteEnd.
     */
    @java.lang.Override
    public boolean getInfiniteEnd() {
      return infiniteEnd_;
    }

    public static final int CLOSED_FIELD_NUMBER = 4;
    private boolean closed_;
    /**
     * <pre>
     * will more data be appended to the file
     * </pre>
     *
     * <code>optional bool closed = 4 [default = false];</code>
     * @return Whether the closed field is set.
     */
    @java.lang.Override
    public boolean hasClosed() {
      return ((bitField0_ & 0x00000008) != 0);
    }
    /**
     * <pre>
     * will more data be appended to the file
     * </pre>
     *
     * <code>optional bool closed = 4 [default = false];</code>
     * @return The closed.
     */
    @java.lang.Override
    public boolean getClosed() {
      return closed_;
    }

    public static final int CREATEDTIME_FIELD_NUMBER = 5;
    private long createdTime_;
    /**
     * <pre>
     * when, in ms, was the file created?
     * </pre>
     *
     * <code>optional int64 createdTime = 5 [default = 0];</code>
     * @return Whether the createdTime field is set.
     */
    @java.lang.Override
    public boolean hasCreatedTime() {
      return ((bitField0_ & 0x00000010) != 0);
    }
    /**
     * <pre>
     * when, in ms, was the file created?
     * </pre>
     *
     * <code>optional int64 createdTime = 5 [default = 0];</code>
     * @return The createdTime.
     */
    @java.lang.Override
    public long getCreatedTime() {
      return createdTime_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeInt64(1, begin_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeInt64(2, end_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeBool(3, infiniteEnd_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeBool(4, closed_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        output.writeInt64(5, createdTime_);
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(1, begin_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(2, end_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(3, infiniteEnd_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(4, closed_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(5, createdTime_);
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof org.apache.accumulo.server.replication.proto.Replication.Status)) {
        return super.equals(obj);
      }
      org.apache.accumulo.server.replication.proto.Replication.Status other = (org.apache.accumulo.server.replication.proto.Replication.Status) obj;

      if (hasBegin() != other.hasBegin()) return false;
      if (hasBegin()) {
        if (getBegin()
            != other.getBegin()) return false;
      }
      if (hasEnd() != other.hasEnd()) return false;
      if (hasEnd()) {
        if (getEnd()
            != other.getEnd()) return false;
      }
      if (hasInfiniteEnd() != other.hasInfiniteEnd()) return false;
      if (hasInfiniteEnd()) {
        if (getInfiniteEnd()
            != other.getInfiniteEnd()) return false;
      }
      if (hasClosed() != other.hasClosed()) return false;
      if (hasClosed()) {
        if (getClosed()
            != other.getClosed()) return false;
      }
      if (hasCreatedTime() != other.hasCreatedTime()) return false;
      if (hasCreatedTime()) {
        if (getCreatedTime()
            != other.getCreatedTime()) return false;
      }
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @SuppressWarnings("unchecked")
    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasBegin()) {
        hash = (37 * hash) + BEGIN_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            getBegin());
      }
      if (hasEnd()) {
        hash = (37 * hash) + END_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            getEnd());
      }
      if (hasInfiniteEnd()) {
        hash = (37 * hash) + INFINITEEND_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
            getInfiniteEnd());
      }
      if (hasClosed()) {
        hash = (37 * hash) + CLOSED_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
            getClosed());
      }
      if (hasCreatedTime()) {
        hash = (37 * hash) + CREATEDTIME_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            getCreatedTime());
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.accumulo.server.replication.proto.Replication.Status parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.accumulo.server.replication.proto.Replication.Status parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.accumulo.server.replication.proto.Replication.Status parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.accumulo.server.replication.proto.Replication.Status parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.accumulo.server.replication.proto.Replication.Status parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.accumulo.server.replication.proto.Replication.Status parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.accumulo.server.replication.proto.Replication.Status parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.accumulo.server.replication.proto.Replication.Status parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.accumulo.server.replication.proto.Replication.Status parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.accumulo.server.replication.proto.Replication.Status parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.accumulo.server.replication.proto.Replication.Status parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.accumulo.server.replication.proto.Replication.Status parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(org.apache.accumulo.server.replication.proto.Replication.Status prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code Status}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:Status)
        org.apache.accumulo.server.replication.proto.Replication.StatusOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.accumulo.server.replication.proto.Replication.internal_static_Status_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.accumulo.server.replication.proto.Replication.internal_static_Status_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.accumulo.server.replication.proto.Replication.Status.class, org.apache.accumulo.server.replication.proto.Replication.Status.Builder.class);
      }

      // Construct using org.apache.accumulo.server.replication.proto.Replication.Status.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        begin_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000001);
        end_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000002);
        infiniteEnd_ = false;
        bitField0_ = (bitField0_ & ~0x00000004);
        closed_ = false;
        bitField0_ = (bitField0_ & ~0x00000008);
        createdTime_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000010);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.accumulo.server.replication.proto.Replication.internal_static_Status_descriptor;
      }

      @java.lang.Override
      public org.apache.accumulo.server.replication.proto.Replication.Status getDefaultInstanceForType() {
        return org.apache.accumulo.server.replication.proto.Replication.Status.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.accumulo.server.replication.proto.Replication.Status build() {
        org.apache.accumulo.server.replication.proto.Replication.Status result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.accumulo.server.replication.proto.Replication.Status buildPartial() {
        org.apache.accumulo.server.replication.proto.Replication.Status result = new org.apache.accumulo.server.replication.proto.Replication.Status(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.begin_ = begin_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.end_ = end_;
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          result.infiniteEnd_ = infiniteEnd_;
          to_bitField0_ |= 0x00000004;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          result.closed_ = closed_;
          to_bitField0_ |= 0x00000008;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          result.createdTime_ = createdTime_;
          to_bitField0_ |= 0x00000010;
        }
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.apache.accumulo.server.replication.proto.Replication.Status) {
          return mergeFrom((org.apache.accumulo.server.replication.proto.Replication.Status)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.accumulo.server.replication.proto.Replication.Status other) {
        if (other == org.apache.accumulo.server.replication.proto.Replication.Status.getDefaultInstance()) return this;
        if (other.hasBegin()) {
          setBegin(other.getBegin());
        }
        if (other.hasEnd()) {
          setEnd(other.getEnd());
        }
        if (other.hasInfiniteEnd()) {
          setInfiniteEnd(other.getInfiniteEnd());
        }
        if (other.hasClosed()) {
          setClosed(other.getClosed());
        }
        if (other.hasCreatedTime()) {
          setCreatedTime(other.getCreatedTime());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.accumulo.server.replication.proto.Replication.Status parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.accumulo.server.replication.proto.Replication.Status) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private long begin_ ;
      /**
       * <pre>
       * offset where replication should start
       * </pre>
       *
       * <code>optional int64 begin = 1 [default = 0];</code>
       * @return Whether the begin field is set.
       */
      @java.lang.Override
      public boolean hasBegin() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <pre>
       * offset where replication should start
       * </pre>
       *
       * <code>optional int64 begin = 1 [default = 0];</code>
       * @return The begin.
       */
      @java.lang.Override
      public long getBegin() {
        return begin_;
      }
      /**
       * <pre>
       * offset where replication should start
       * </pre>
       *
       * <code>optional int64 begin = 1 [default = 0];</code>
       * @param value The begin to set.
       * @return This builder for chaining.
       */
      public Builder setBegin(long value) {
        bitField0_ |= 0x00000001;
        begin_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * offset where replication should start
       * </pre>
       *
       * <code>optional int64 begin = 1 [default = 0];</code>
       * @return This builder for chaining.
       */
      public Builder clearBegin() {
        bitField0_ = (bitField0_ & ~0x00000001);
        begin_ = 0L;
        onChanged();
        return this;
      }

      private long end_ ;
      /**
       * <pre>
       * offset where data is ready for replication
       * </pre>
       *
       * <code>optional int64 end = 2 [default = 0];</code>
       * @return Whether the end field is set.
       */
      @java.lang.Override
      public boolean hasEnd() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <pre>
       * offset where data is ready for replication
       * </pre>
       *
       * <code>optional int64 end = 2 [default = 0];</code>
       * @return The end.
       */
      @java.lang.Override
      public long getEnd() {
        return end_;
      }
      /**
       * <pre>
       * offset where data is ready for replication
       * </pre>
       *
       * <code>optional int64 end = 2 [default = 0];</code>
       * @param value The end to set.
       * @return This builder for chaining.
       */
      public Builder setEnd(long value) {
        bitField0_ |= 0x00000002;
        end_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * offset where data is ready for replication
       * </pre>
       *
       * <code>optional int64 end = 2 [default = 0];</code>
       * @return This builder for chaining.
       */
      public Builder clearEnd() {
        bitField0_ = (bitField0_ & ~0x00000002);
        end_ = 0L;
        onChanged();
        return this;
      }

      private boolean infiniteEnd_ ;
      /**
       * <pre>
       * do we have a discrete 'end'
       * </pre>
       *
       * <code>optional bool infiniteEnd = 3 [default = false];</code>
       * @return Whether the infiniteEnd field is set.
       */
      @java.lang.Override
      public boolean hasInfiniteEnd() {
        return ((bitField0_ & 0x00000004) != 0);
      }
      /**
       * <pre>
       * do we have a discrete 'end'
       * </pre>
       *
       * <code>optional bool infiniteEnd = 3 [default = false];</code>
       * @return The infiniteEnd.
       */
      @java.lang.Override
      public boolean getInfiniteEnd() {
        return infiniteEnd_;
      }
      /**
       * <pre>
       * do we have a discrete 'end'
       * </pre>
       *
       * <code>optional bool infiniteEnd = 3 [default = false];</code>
       * @param value The infiniteEnd to set.
       * @return This builder for chaining.
       */
      public Builder setInfiniteEnd(boolean value) {
        bitField0_ |= 0x00000004;
        infiniteEnd_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * do we have a discrete 'end'
       * </pre>
       *
       * <code>optional bool infiniteEnd = 3 [default = false];</code>
       * @return This builder for chaining.
       */
      public Builder clearInfiniteEnd() {
        bitField0_ = (bitField0_ & ~0x00000004);
        infiniteEnd_ = false;
        onChanged();
        return this;
      }

      private boolean closed_ ;
      /**
       * <pre>
       * will more data be appended to the file
       * </pre>
       *
       * <code>optional bool closed = 4 [default = false];</code>
       * @return Whether the closed field is set.
       */
      @java.lang.Override
      public boolean hasClosed() {
        return ((bitField0_ & 0x00000008) != 0);
      }
      /**
       * <pre>
       * will more data be appended to the file
       * </pre>
       *
       * <code>optional bool closed = 4 [default = false];</code>
       * @return The closed.
       */
      @java.lang.Override
      public boolean getClosed() {
        return closed_;
      }
      /**
       * <pre>
       * will more data be appended to the file
       * </pre>
       *
       * <code>optional bool closed = 4 [default = false];</code>
       * @param value The closed to set.
       * @return This builder for chaining.
       */
      public Builder setClosed(boolean value) {
        bitField0_ |= 0x00000008;
        closed_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * will more data be appended to the file
       * </pre>
       *
       * <code>optional bool closed = 4 [default = false];</code>
       * @return This builder for chaining.
       */
      public Builder clearClosed() {
        bitField0_ = (bitField0_ & ~0x00000008);
        closed_ = false;
        onChanged();
        return this;
      }

      private long createdTime_ ;
      /**
       * <pre>
       * when, in ms, was the file created?
       * </pre>
       *
       * <code>optional int64 createdTime = 5 [default = 0];</code>
       * @return Whether the createdTime field is set.
       */
      @java.lang.Override
      public boolean hasCreatedTime() {
        return ((bitField0_ & 0x00000010) != 0);
      }
      /**
       * <pre>
       * when, in ms, was the file created?
       * </pre>
       *
       * <code>optional int64 createdTime = 5 [default = 0];</code>
       * @return The createdTime.
       */
      @java.lang.Override
      public long getCreatedTime() {
        return createdTime_;
      }
      /**
       * <pre>
       * when, in ms, was the file created?
       * </pre>
       *
       * <code>optional int64 createdTime = 5 [default = 0];</code>
       * @param value The createdTime to set.
       * @return This builder for chaining.
       */
      public Builder setCreatedTime(long value) {
        bitField0_ |= 0x00000010;
        createdTime_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * when, in ms, was the file created?
       * </pre>
       *
       * <code>optional int64 createdTime = 5 [default = 0];</code>
       * @return This builder for chaining.
       */
      public Builder clearCreatedTime() {
        bitField0_ = (bitField0_ & ~0x00000010);
        createdTime_ = 0L;
        onChanged();
        return this;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:Status)
    }

    // @@protoc_insertion_point(class_scope:Status)
    private static final org.apache.accumulo.server.replication.proto.Replication.Status DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.accumulo.server.replication.proto.Replication.Status();
    }

    public static org.apache.accumulo.server.replication.proto.Replication.Status getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<Status>
        PARSER = new com.google.protobuf.AbstractParser<Status>() {
      @java.lang.Override
      public Status parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Status(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<Status> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<Status> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.accumulo.server.replication.proto.Replication.Status getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Status_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Status_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n#src/main/protobuf/replication.proto\"u\n" +
      "\006Status\022\020\n\005begin\030\001 \001(\003:\0010\022\016\n\003end\030\002 \001(\003:\001" +
      "0\022\032\n\013infiniteEnd\030\003 \001(\010:\005false\022\025\n\006closed\030" +
      "\004 \001(\010:\005false\022\026\n\013createdTime\030\005 \001(\003:\0010B0\n," +
      "org.apache.accumulo.server.replication.p" +
      "rotoH\001"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_Status_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_Status_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Status_descriptor,
        new java.lang.String[] { "Begin", "End", "InfiniteEnd", "Closed", "CreatedTime", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
