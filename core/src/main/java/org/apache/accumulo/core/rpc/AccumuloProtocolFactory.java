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
package org.apache.accumulo.core.rpc;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

/**
 * Factory for creating instances of the AccumuloProtocol.
 * <p>
 * This protocol includes a custom header to ensure compatibility between different versions of the
 * protocol. It also traces RPC calls.
 */
public class AccumuloProtocolFactory extends TCompactProtocol.Factory {

  private static final long serialVersionUID = 1L;

  private final boolean isClient;
  private final InstanceId instanceId;

  public static class AccumuloProtocol extends TCompactProtocol {

    static final int MAGIC_NUMBER = 0x41434355; // "ACCU" in ASCII
    static final byte PROTOCOL_VERSION = 1;

    private final boolean isClient;
    private final InstanceId instanceId;

    private Span span = null;
    private Scope scope = null;

    public AccumuloProtocol(TTransport transport, InstanceId instanceId, boolean isClient) {
      super(transport);
      this.instanceId = instanceId;
      this.isClient = isClient;
    }

    /**
     * For client calls, write the Accumulo protocol header before writing the message
     */
    @Override
    public void writeMessageBegin(TMessage message) throws TException {
      span = TraceUtil.startClientRpcSpan(this.getClass(), message.name);
      scope = span.makeCurrent();

      if (this.isClient) {
        this.writeClientHeader();
      }
      super.writeMessageBegin(message);
    }

    @Override
    public void writeMessageEnd() throws TException {
      super.writeMessageEnd();
      scope.close();
      span.end();
    }

    /**
     * For server calls, validate the header before reading the message
     */
    @Override
    public TMessage readMessageBegin() throws TException {
      if (!this.isClient) {
        this.validateHeader();
      }

      return super.readMessageBegin();
    }

    /**
     * Writes the Accumulo protocol header containing version and identification info
     */
    private void writeClientHeader() throws TException {
      super.writeI32(MAGIC_NUMBER);
      super.writeByte(PROTOCOL_VERSION);
      super.writeString(Constants.VERSION);
      super.writeString(this.instanceId.canonical());
    }

    /**
     * Reads and validates the Accumulo protocol header
     *
     * @throws TException if the header is invalid or incompatible
     */
    void validateHeader() throws TException {

      final int magic;
      try {
        magic = super.readI32();
      } catch (TException e) {
        throw new TException("Failed to read magic number from header", e);
      }
      if (magic != MAGIC_NUMBER) {
        throw new TException("Invalid Accumulo protocol: magic number mismatch. Expected: 0x"
            + Integer.toHexString(MAGIC_NUMBER) + ", got: 0x" + Integer.toHexString(magic));
      }

      final byte clientProtocolVersion;
      try {
        clientProtocolVersion = super.readByte();
      } catch (TException e) {
        throw new TException("Failed to read protocol version from header", e);
      }
      validateProtocolVersion(clientProtocolVersion);

      final String clientAccumuloVersion;
      try {
        clientAccumuloVersion = super.readString();
      } catch (TException e) {
        throw new TException("Failed to read accumulo version from header", e);
      }
      validateAccumuloVersion(clientAccumuloVersion);

      final String clientInstanceId;
      try {
        clientInstanceId = super.readString();
      } catch (TException e) {
        throw new TException("Failed to read instance id from header", e);
      }
      validateInstanceId(clientInstanceId);
    }

    /**
     * @throws TException if the given protocol version is incompatible with the current version
     */
    private void validateProtocolVersion(byte protocolVersion) throws TException {
      if (protocolVersion != PROTOCOL_VERSION) {
        throw new TException("Incompatible protocol version. Version seen: " + protocolVersion
            + ", expected version: " + PROTOCOL_VERSION);
      }
    }

    /**
     * @throws TException if the given Accumulo version (client) is incompatible with the current
     *         version (server)
     */
    private void validateAccumuloVersion(String clientAccumuloVersion) throws TException {
      final String serverAccumuloVersion = Constants.VERSION;

      // Extract major.minor version components
      final String serverMajorMinor = extractMajorMinorVersion(serverAccumuloVersion);
      final String clientMajorMinor = extractMajorMinorVersion(clientAccumuloVersion);

      if (!serverMajorMinor.equals(clientMajorMinor)) {
        throw new TException("Incompatible Accumulo versions. Client version: "
            + clientAccumuloVersion + ", Server version: " + serverAccumuloVersion
            + ". Major.minor versions must match.");
      }
    }

    /**
     * @return the major.minor portion from a version string (e.g., "4.0.0-SNAPSHOT" -> "4.0")
     */
    private String extractMajorMinorVersion(String version) throws TException {
      final int lastDotIndex = version.lastIndexOf('.');
      if (lastDotIndex == -1) {
        throw new TException("Invalid version format: " + version);
      }
      return version.substring(0, lastDotIndex);
    }

    /**
     * @throws TException if the given instance ID (client) does not match the current instance ID
     *         (server)
     */
    private void validateInstanceId(String clientInstanceId) throws TException {
      InstanceId clientId;
      try {
        clientId = InstanceId.of(clientInstanceId);
      } catch (IllegalArgumentException e) {
        throw new TException("Invalid instance ID in header: " + clientInstanceId, e);
      }
      if (!clientId.equals(this.instanceId)) {
        throw new TException(
            "Mismatched instance ID in header. Expected to match server instance ID: "
                + this.instanceId + ", but got: " + clientId);
      }
    }

  }

  @Override
  public TProtocol getProtocol(TTransport trans) {
    return new AccumuloProtocol(trans, this.instanceId, isClient);
  }

  /**
   * Creates a factory for producing AccumuloProtocol instances
   *
   * @param instanceId the instance ID of the client or server
   * @param isClient true if this factory produces protocols for the client side, false for the
   *        server side
   */
  private AccumuloProtocolFactory(InstanceId instanceId, boolean isClient) {
    this.isClient = isClient;
    this.instanceId = instanceId;
  }

  /**
   * Creates a client-side factory for use in clients making RPC calls
   */
  public static AccumuloProtocolFactory clientFactory(InstanceId instanceId) {
    return new AccumuloProtocolFactory(instanceId, true);
  }

  /**
   * Creates a server-side factory for use in servers receiving RPC calls
   */
  public static AccumuloProtocolFactory serverFactory(InstanceId instanceId) {
    return new AccumuloProtocolFactory(instanceId, false);
  }
}
