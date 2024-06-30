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
// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: compaction-coordinator.proto

// Protobuf Java Version: 3.25.3
package org.apache.accumulo.core.compaction.protobuf;

/**
 * <pre>
 * There are no nulls with Protobuf3, so the default will be an empty string
 * Our TCredentials version with thrift currently is using/checking for null so
 * using the optional field will generate "has" methods which we can use to
 * check if the value was set
 * </pre>
 *
 * Protobuf type {@code compaction_coordinator.PCredentials}
 */
public final class PCredentials extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:compaction_coordinator.PCredentials)
    PCredentialsOrBuilder {
private static final long serialVersionUID = 0L;
  // Use PCredentials.newBuilder() to construct.
  private PCredentials(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private PCredentials() {
    principal_ = "";
    tokenClassName_ = "";
    token_ = com.google.protobuf.ByteString.EMPTY;
    instanceId_ = "";
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new PCredentials();
  }

  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return org.apache.accumulo.core.compaction.protobuf.CompactionCoordinatorServiceProto.internal_static_compaction_coordinator_PCredentials_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return org.apache.accumulo.core.compaction.protobuf.CompactionCoordinatorServiceProto.internal_static_compaction_coordinator_PCredentials_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            org.apache.accumulo.core.compaction.protobuf.PCredentials.class, org.apache.accumulo.core.compaction.protobuf.PCredentials.Builder.class);
  }

  private int bitField0_;
  public static final int PRINCIPAL_FIELD_NUMBER = 1;
  @SuppressWarnings("serial")
  private volatile java.lang.Object principal_ = "";
  /**
   * <code>optional string principal = 1;</code>
   * @return Whether the principal field is set.
   */
  @java.lang.Override
  public boolean hasPrincipal() {
    return ((bitField0_ & 0x00000001) != 0);
  }
  /**
   * <code>optional string principal = 1;</code>
   * @return The principal.
   */
  @java.lang.Override
  public java.lang.String getPrincipal() {
    java.lang.Object ref = principal_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      principal_ = s;
      return s;
    }
  }
  /**
   * <code>optional string principal = 1;</code>
   * @return The bytes for principal.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString
      getPrincipalBytes() {
    java.lang.Object ref = principal_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      principal_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int TOKENCLASSNAME_FIELD_NUMBER = 2;
  @SuppressWarnings("serial")
  private volatile java.lang.Object tokenClassName_ = "";
  /**
   * <code>optional string tokenClassName = 2;</code>
   * @return Whether the tokenClassName field is set.
   */
  @java.lang.Override
  public boolean hasTokenClassName() {
    return ((bitField0_ & 0x00000002) != 0);
  }
  /**
   * <code>optional string tokenClassName = 2;</code>
   * @return The tokenClassName.
   */
  @java.lang.Override
  public java.lang.String getTokenClassName() {
    java.lang.Object ref = tokenClassName_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      tokenClassName_ = s;
      return s;
    }
  }
  /**
   * <code>optional string tokenClassName = 2;</code>
   * @return The bytes for tokenClassName.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString
      getTokenClassNameBytes() {
    java.lang.Object ref = tokenClassName_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      tokenClassName_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int TOKEN_FIELD_NUMBER = 3;
  private com.google.protobuf.ByteString token_ = com.google.protobuf.ByteString.EMPTY;
  /**
   * <code>optional bytes token = 3;</code>
   * @return Whether the token field is set.
   */
  @java.lang.Override
  public boolean hasToken() {
    return ((bitField0_ & 0x00000004) != 0);
  }
  /**
   * <code>optional bytes token = 3;</code>
   * @return The token.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString getToken() {
    return token_;
  }

  public static final int INSTANCEID_FIELD_NUMBER = 4;
  @SuppressWarnings("serial")
  private volatile java.lang.Object instanceId_ = "";
  /**
   * <code>optional string instanceId = 4;</code>
   * @return Whether the instanceId field is set.
   */
  @java.lang.Override
  public boolean hasInstanceId() {
    return ((bitField0_ & 0x00000008) != 0);
  }
  /**
   * <code>optional string instanceId = 4;</code>
   * @return The instanceId.
   */
  @java.lang.Override
  public java.lang.String getInstanceId() {
    java.lang.Object ref = instanceId_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      instanceId_ = s;
      return s;
    }
  }
  /**
   * <code>optional string instanceId = 4;</code>
   * @return The bytes for instanceId.
   */
  @java.lang.Override
  public com.google.protobuf.ByteString
      getInstanceIdBytes() {
    java.lang.Object ref = instanceId_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      instanceId_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
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
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, principal_);
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, tokenClassName_);
    }
    if (((bitField0_ & 0x00000004) != 0)) {
      output.writeBytes(3, token_);
    }
    if (((bitField0_ & 0x00000008) != 0)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 4, instanceId_);
    }
    getUnknownFields().writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) != 0)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, principal_);
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, tokenClassName_);
    }
    if (((bitField0_ & 0x00000004) != 0)) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(3, token_);
    }
    if (((bitField0_ & 0x00000008) != 0)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(4, instanceId_);
    }
    size += getUnknownFields().getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof org.apache.accumulo.core.compaction.protobuf.PCredentials)) {
      return super.equals(obj);
    }
    org.apache.accumulo.core.compaction.protobuf.PCredentials other = (org.apache.accumulo.core.compaction.protobuf.PCredentials) obj;

    if (hasPrincipal() != other.hasPrincipal()) return false;
    if (hasPrincipal()) {
      if (!getPrincipal()
          .equals(other.getPrincipal())) return false;
    }
    if (hasTokenClassName() != other.hasTokenClassName()) return false;
    if (hasTokenClassName()) {
      if (!getTokenClassName()
          .equals(other.getTokenClassName())) return false;
    }
    if (hasToken() != other.hasToken()) return false;
    if (hasToken()) {
      if (!getToken()
          .equals(other.getToken())) return false;
    }
    if (hasInstanceId() != other.hasInstanceId()) return false;
    if (hasInstanceId()) {
      if (!getInstanceId()
          .equals(other.getInstanceId())) return false;
    }
    if (!getUnknownFields().equals(other.getUnknownFields())) return false;
    return true;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    if (hasPrincipal()) {
      hash = (37 * hash) + PRINCIPAL_FIELD_NUMBER;
      hash = (53 * hash) + getPrincipal().hashCode();
    }
    if (hasTokenClassName()) {
      hash = (37 * hash) + TOKENCLASSNAME_FIELD_NUMBER;
      hash = (53 * hash) + getTokenClassName().hashCode();
    }
    if (hasToken()) {
      hash = (37 * hash) + TOKEN_FIELD_NUMBER;
      hash = (53 * hash) + getToken().hashCode();
    }
    if (hasInstanceId()) {
      hash = (37 * hash) + INSTANCEID_FIELD_NUMBER;
      hash = (53 * hash) + getInstanceId().hashCode();
    }
    hash = (29 * hash) + getUnknownFields().hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static org.apache.accumulo.core.compaction.protobuf.PCredentials parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.apache.accumulo.core.compaction.protobuf.PCredentials parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.apache.accumulo.core.compaction.protobuf.PCredentials parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.apache.accumulo.core.compaction.protobuf.PCredentials parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.apache.accumulo.core.compaction.protobuf.PCredentials parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.apache.accumulo.core.compaction.protobuf.PCredentials parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.apache.accumulo.core.compaction.protobuf.PCredentials parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static org.apache.accumulo.core.compaction.protobuf.PCredentials parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public static org.apache.accumulo.core.compaction.protobuf.PCredentials parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }

  public static org.apache.accumulo.core.compaction.protobuf.PCredentials parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static org.apache.accumulo.core.compaction.protobuf.PCredentials parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static org.apache.accumulo.core.compaction.protobuf.PCredentials parseFrom(
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
  public static Builder newBuilder(org.apache.accumulo.core.compaction.protobuf.PCredentials prototype) {
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
   * <pre>
   * There are no nulls with Protobuf3, so the default will be an empty string
   * Our TCredentials version with thrift currently is using/checking for null so
   * using the optional field will generate "has" methods which we can use to
   * check if the value was set
   * </pre>
   *
   * Protobuf type {@code compaction_coordinator.PCredentials}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:compaction_coordinator.PCredentials)
      org.apache.accumulo.core.compaction.protobuf.PCredentialsOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.accumulo.core.compaction.protobuf.CompactionCoordinatorServiceProto.internal_static_compaction_coordinator_PCredentials_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.accumulo.core.compaction.protobuf.CompactionCoordinatorServiceProto.internal_static_compaction_coordinator_PCredentials_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.accumulo.core.compaction.protobuf.PCredentials.class, org.apache.accumulo.core.compaction.protobuf.PCredentials.Builder.class);
    }

    // Construct using org.apache.accumulo.core.compaction.protobuf.PCredentials.newBuilder()
    private Builder() {

    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);

    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      bitField0_ = 0;
      principal_ = "";
      tokenClassName_ = "";
      token_ = com.google.protobuf.ByteString.EMPTY;
      instanceId_ = "";
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return org.apache.accumulo.core.compaction.protobuf.CompactionCoordinatorServiceProto.internal_static_compaction_coordinator_PCredentials_descriptor;
    }

    @java.lang.Override
    public org.apache.accumulo.core.compaction.protobuf.PCredentials getDefaultInstanceForType() {
      return org.apache.accumulo.core.compaction.protobuf.PCredentials.getDefaultInstance();
    }

    @java.lang.Override
    public org.apache.accumulo.core.compaction.protobuf.PCredentials build() {
      org.apache.accumulo.core.compaction.protobuf.PCredentials result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public org.apache.accumulo.core.compaction.protobuf.PCredentials buildPartial() {
      org.apache.accumulo.core.compaction.protobuf.PCredentials result = new org.apache.accumulo.core.compaction.protobuf.PCredentials(this);
      if (bitField0_ != 0) { buildPartial0(result); }
      onBuilt();
      return result;
    }

    private void buildPartial0(org.apache.accumulo.core.compaction.protobuf.PCredentials result) {
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) != 0)) {
        result.principal_ = principal_;
        to_bitField0_ |= 0x00000001;
      }
      if (((from_bitField0_ & 0x00000002) != 0)) {
        result.tokenClassName_ = tokenClassName_;
        to_bitField0_ |= 0x00000002;
      }
      if (((from_bitField0_ & 0x00000004) != 0)) {
        result.token_ = token_;
        to_bitField0_ |= 0x00000004;
      }
      if (((from_bitField0_ & 0x00000008) != 0)) {
        result.instanceId_ = instanceId_;
        to_bitField0_ |= 0x00000008;
      }
      result.bitField0_ |= to_bitField0_;
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
      if (other instanceof org.apache.accumulo.core.compaction.protobuf.PCredentials) {
        return mergeFrom((org.apache.accumulo.core.compaction.protobuf.PCredentials)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(org.apache.accumulo.core.compaction.protobuf.PCredentials other) {
      if (other == org.apache.accumulo.core.compaction.protobuf.PCredentials.getDefaultInstance()) return this;
      if (other.hasPrincipal()) {
        principal_ = other.principal_;
        bitField0_ |= 0x00000001;
        onChanged();
      }
      if (other.hasTokenClassName()) {
        tokenClassName_ = other.tokenClassName_;
        bitField0_ |= 0x00000002;
        onChanged();
      }
      if (other.hasToken()) {
        setToken(other.getToken());
      }
      if (other.hasInstanceId()) {
        instanceId_ = other.instanceId_;
        bitField0_ |= 0x00000008;
        onChanged();
      }
      this.mergeUnknownFields(other.getUnknownFields());
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
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 10: {
              principal_ = input.readStringRequireUtf8();
              bitField0_ |= 0x00000001;
              break;
            } // case 10
            case 18: {
              tokenClassName_ = input.readStringRequireUtf8();
              bitField0_ |= 0x00000002;
              break;
            } // case 18
            case 26: {
              token_ = input.readBytes();
              bitField0_ |= 0x00000004;
              break;
            } // case 26
            case 34: {
              instanceId_ = input.readStringRequireUtf8();
              bitField0_ |= 0x00000008;
              break;
            } // case 34
            default: {
              if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                done = true; // was an endgroup tag
              }
              break;
            } // default:
          } // switch (tag)
        } // while (!done)
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.unwrapIOException();
      } finally {
        onChanged();
      } // finally
      return this;
    }
    private int bitField0_;

    private java.lang.Object principal_ = "";
    /**
     * <code>optional string principal = 1;</code>
     * @return Whether the principal field is set.
     */
    public boolean hasPrincipal() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional string principal = 1;</code>
     * @return The principal.
     */
    public java.lang.String getPrincipal() {
      java.lang.Object ref = principal_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        principal_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string principal = 1;</code>
     * @return The bytes for principal.
     */
    public com.google.protobuf.ByteString
        getPrincipalBytes() {
      java.lang.Object ref = principal_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        principal_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string principal = 1;</code>
     * @param value The principal to set.
     * @return This builder for chaining.
     */
    public Builder setPrincipal(
        java.lang.String value) {
      if (value == null) { throw new NullPointerException(); }
      principal_ = value;
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }
    /**
     * <code>optional string principal = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearPrincipal() {
      principal_ = getDefaultInstance().getPrincipal();
      bitField0_ = (bitField0_ & ~0x00000001);
      onChanged();
      return this;
    }
    /**
     * <code>optional string principal = 1;</code>
     * @param value The bytes for principal to set.
     * @return This builder for chaining.
     */
    public Builder setPrincipalBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      checkByteStringIsUtf8(value);
      principal_ = value;
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }

    private java.lang.Object tokenClassName_ = "";
    /**
     * <code>optional string tokenClassName = 2;</code>
     * @return Whether the tokenClassName field is set.
     */
    public boolean hasTokenClassName() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional string tokenClassName = 2;</code>
     * @return The tokenClassName.
     */
    public java.lang.String getTokenClassName() {
      java.lang.Object ref = tokenClassName_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        tokenClassName_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string tokenClassName = 2;</code>
     * @return The bytes for tokenClassName.
     */
    public com.google.protobuf.ByteString
        getTokenClassNameBytes() {
      java.lang.Object ref = tokenClassName_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        tokenClassName_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string tokenClassName = 2;</code>
     * @param value The tokenClassName to set.
     * @return This builder for chaining.
     */
    public Builder setTokenClassName(
        java.lang.String value) {
      if (value == null) { throw new NullPointerException(); }
      tokenClassName_ = value;
      bitField0_ |= 0x00000002;
      onChanged();
      return this;
    }
    /**
     * <code>optional string tokenClassName = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearTokenClassName() {
      tokenClassName_ = getDefaultInstance().getTokenClassName();
      bitField0_ = (bitField0_ & ~0x00000002);
      onChanged();
      return this;
    }
    /**
     * <code>optional string tokenClassName = 2;</code>
     * @param value The bytes for tokenClassName to set.
     * @return This builder for chaining.
     */
    public Builder setTokenClassNameBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      checkByteStringIsUtf8(value);
      tokenClassName_ = value;
      bitField0_ |= 0x00000002;
      onChanged();
      return this;
    }

    private com.google.protobuf.ByteString token_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <code>optional bytes token = 3;</code>
     * @return Whether the token field is set.
     */
    @java.lang.Override
    public boolean hasToken() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <code>optional bytes token = 3;</code>
     * @return The token.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString getToken() {
      return token_;
    }
    /**
     * <code>optional bytes token = 3;</code>
     * @param value The token to set.
     * @return This builder for chaining.
     */
    public Builder setToken(com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      token_ = value;
      bitField0_ |= 0x00000004;
      onChanged();
      return this;
    }
    /**
     * <code>optional bytes token = 3;</code>
     * @return This builder for chaining.
     */
    public Builder clearToken() {
      bitField0_ = (bitField0_ & ~0x00000004);
      token_ = getDefaultInstance().getToken();
      onChanged();
      return this;
    }

    private java.lang.Object instanceId_ = "";
    /**
     * <code>optional string instanceId = 4;</code>
     * @return Whether the instanceId field is set.
     */
    public boolean hasInstanceId() {
      return ((bitField0_ & 0x00000008) != 0);
    }
    /**
     * <code>optional string instanceId = 4;</code>
     * @return The instanceId.
     */
    public java.lang.String getInstanceId() {
      java.lang.Object ref = instanceId_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        instanceId_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string instanceId = 4;</code>
     * @return The bytes for instanceId.
     */
    public com.google.protobuf.ByteString
        getInstanceIdBytes() {
      java.lang.Object ref = instanceId_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        instanceId_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string instanceId = 4;</code>
     * @param value The instanceId to set.
     * @return This builder for chaining.
     */
    public Builder setInstanceId(
        java.lang.String value) {
      if (value == null) { throw new NullPointerException(); }
      instanceId_ = value;
      bitField0_ |= 0x00000008;
      onChanged();
      return this;
    }
    /**
     * <code>optional string instanceId = 4;</code>
     * @return This builder for chaining.
     */
    public Builder clearInstanceId() {
      instanceId_ = getDefaultInstance().getInstanceId();
      bitField0_ = (bitField0_ & ~0x00000008);
      onChanged();
      return this;
    }
    /**
     * <code>optional string instanceId = 4;</code>
     * @param value The bytes for instanceId to set.
     * @return This builder for chaining.
     */
    public Builder setInstanceIdBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      checkByteStringIsUtf8(value);
      instanceId_ = value;
      bitField0_ |= 0x00000008;
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


    // @@protoc_insertion_point(builder_scope:compaction_coordinator.PCredentials)
  }

  // @@protoc_insertion_point(class_scope:compaction_coordinator.PCredentials)
  private static final org.apache.accumulo.core.compaction.protobuf.PCredentials DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new org.apache.accumulo.core.compaction.protobuf.PCredentials();
  }

  public static org.apache.accumulo.core.compaction.protobuf.PCredentials getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<PCredentials>
      PARSER = new com.google.protobuf.AbstractParser<PCredentials>() {
    @java.lang.Override
    public PCredentials parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      Builder builder = newBuilder();
      try {
        builder.mergeFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(builder.buildPartial());
      } catch (com.google.protobuf.UninitializedMessageException e) {
        throw e.asInvalidProtocolBufferException().setUnfinishedMessage(builder.buildPartial());
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(e)
            .setUnfinishedMessage(builder.buildPartial());
      }
      return builder.buildPartial();
    }
  };

  public static com.google.protobuf.Parser<PCredentials> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<PCredentials> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public org.apache.accumulo.core.compaction.protobuf.PCredentials getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

