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

import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

/**
 * Factory for creating instances of the AccumuloProtocol.
 * <p>
 * This protocol includes a custom header to ensure compatibility between different versions of the
 * protocol.
 */
public class AccumuloProtocolFactory extends TCompactProtocol.Factory {

  private static final long serialVersionUID = 1L;

  private final boolean isClient;

  public static class AccumuloProtocol extends TCompactProtocol {

    static final int MAGIC_NUMBER = 0x41434355; // "ACCU" in ASCII
    static final byte PROTOCOL_VERSION = 1;

    private final boolean isClient;

    private Span span = null;
    private Scope scope = null;

    public AccumuloProtocol(TTransport transport, boolean isClient) {
      super(transport);
      this.isClient = isClient;
    }

    /**
     * For client calls, add RPC span and write the validation header
     */
    @Override
    public void writeMessageBegin(TMessage message) throws TException {
      if (!this.isClient) {
        super.writeMessageBegin(message);
      } else {
        span = TraceUtil.startClientRpcSpan(this.getClass(), message.name);
        scope = span.makeCurrent();

        try {
          this.writeClientHeader();
          super.writeMessageBegin(message);
        } catch (TException e) {
          if (span != null) {
            TraceUtil.setException(span, e, false);
          }
          if (scope != null) {
            scope.close();
            scope = null;
          }
          if (span != null) {
            span.end();
            span = null;
          }
          throw e;
        }
      }
    }

    /**
     * Writes the Accumulo protocol header containing version and identification info
     */
    private void writeClientHeader() throws TException {
      super.writeI32(MAGIC_NUMBER);
      super.writeByte(PROTOCOL_VERSION);

      final boolean headerHasTrace = span != null && span.getSpanContext().isValid();
      super.writeBool(headerHasTrace);

      if (headerHasTrace) {
        String serializedContext = TraceUtil.serializeContext(Context.current());
        super.writeString(serializedContext);
      }
    }

    @Override
    public void writeMessageEnd() throws TException {
      try {
        super.writeMessageEnd();
      } finally {
        if (scope != null) {
          scope.close();
          scope = null;
        }
        if (span != null) {
          span.end();
          span = null;
        }
      }
    }

    /**
     * For server calls, validate the header
     */
    @Override
    public TMessage readMessageBegin() throws TException {
      if (!this.isClient) {
        this.validateHeader();
      }

      return super.readMessageBegin();
    }

    /**
     * Reads and validates the Accumulo protocol header
     *
     * @throws TException if the header is invalid or incompatible
     */
    void validateHeader() throws TException {
      final int magic = super.readI32();
      if (magic != MAGIC_NUMBER) {
        throw new TException("Invalid Accumulo protocol: magic number mismatch. Expected: 0x"
            + Integer.toHexString(MAGIC_NUMBER) + ", got: 0x" + Integer.toHexString(magic));
      }

      final byte version = super.readByte();
      validateProtocolVersion(version);

      final boolean headerHasTrace = super.readBool();

      if (headerHasTrace) {
        String serializedContext = super.readString();
        Context extractedContext = TraceUtil.deserializeContext(serializedContext);

        // Create server span with extracted context as parent
        span = TraceUtil.startServerRpcSpanFromContext(this.getClass(), "handleMessage",
            extractedContext);
        scope = span.makeCurrent();
      }
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

  }

  @Override
  public TProtocol getProtocol(TTransport trans) {
    return new AccumuloProtocol(trans, isClient);
  }

  /**
   * Creates a factory for producing AccumuloProtocol instances
   *
   * @param isClient true if this factory produces protocols for the client side, false for the
   *        server side
   */
  private AccumuloProtocolFactory(boolean isClient) {
    this.isClient = isClient;
  }

  /**
   * Creates a client-side factory for use in clients making RPC calls
   */
  public static AccumuloProtocolFactory clientFactory() {
    return new AccumuloProtocolFactory(true);
  }

  /**
   * Creates a server-side factory for use in servers receiving RPC calls
   */
  public static AccumuloProtocolFactory serverFactory() {
    return new AccumuloProtocolFactory(false);
  }
}
