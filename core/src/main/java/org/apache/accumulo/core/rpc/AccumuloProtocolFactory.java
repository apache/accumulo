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

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;

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

    private static final int MAGIC_NUMBER = 0x41434355; // "ACCU" in ASCII
    private static final byte PROTOCOL_VERSION = 1;
    private static final boolean HEADER_HAS_TRACE = true;

    private final boolean isClient;

    private Span span = null;
    private Scope scope = null;
    private final Map<String,String> traceHeaders = new HashMap<>();

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
          this.writeHeader();
          super.writeMessageBegin(message);
        } catch (TException e) {
          if (span != null) {
            span.recordException(e);
            span.setAttribute("error.type", e.getClass().getCanonicalName());
            span.setStatus(StatusCode.ERROR, e.getMessage());
          }
          if (scope != null) {
            scope.close();
          }
          if (span != null) {
            span.end();
          }
          throw e;
        }
      }
    }

    /**
     * Writes the Accumulo protocol header containing version and identification info
     */
    private void writeHeader() throws TException {
      super.writeI32(MAGIC_NUMBER);
      super.writeByte(PROTOCOL_VERSION);

      if (this.isClient && span != null && span.getSpanContext().isValid()) {
        super.writeBool(HEADER_HAS_TRACE);
        traceHeaders.clear();

        W3CTraceContextPropagator.getInstance().inject(Context.current(), traceHeaders,
            (headers, key, value) -> headers.put(key, value));

        super.writeI16((short) traceHeaders.size());

        for (Map.Entry<String,String> entry : traceHeaders.entrySet()) {
          super.writeString(entry.getKey());
          super.writeString(entry.getValue());
        }
      } else {
        super.writeBool(false);
      }
    }

    @Override
    public void writeMessageEnd() throws TException {
      try {
        super.writeMessageEnd();
      } finally {
        if (scope != null) {
          scope.close();
          span.end();
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
    private void validateHeader() throws TException {
      final int magic = super.readI32();
      if (magic != MAGIC_NUMBER) {
        throw new TException("Invalid Accumulo protocol: magic number mismatch. Expected: 0x"
            + Integer.toHexString(MAGIC_NUMBER) + ", got: 0x" + Integer.toHexString(magic));
      }

      final byte version = super.readByte();
      if (!isCompatibleVersion(version)) {
        throw new TException("Incompatible protocol version. Client version: " + version
            + ", Server version: " + PROTOCOL_VERSION);
      }

      final boolean hasTrace = super.readBool();

      if (hasTrace) {
        final short numHeaders = super.readI16();

        final Map<String,String> headers = new HashMap<>(numHeaders);
        for (int i = 0; i < numHeaders; i++) {
          String key = super.readString();
          String value = super.readString();
          headers.put(key, value);
        }

        if (!headers.isEmpty()) {
          Context extractedContext = W3CTraceContextPropagator.getInstance()
              .extract(Context.current(), headers, new TextMapGetter<>() {
                @Override
                public Iterable<String> keys(Map<String,String> carrier) {
                  return carrier.keySet();
                }

                @Override
                public String get(Map<String,String> carrier, String key) {
                  return carrier.get(key);
                }
              });

          // Create server span with extracted context as parent
          span = TraceUtil.startServerRpcSpanFromContext(this.getClass(), "handleMessage",
              extractedContext);
          scope = span.makeCurrent();
        }
      }
    }

    /**
     * Checks if the given version is compatible with the current protocol version
     */
    private boolean isCompatibleVersion(byte version) {
      return version == PROTOCOL_VERSION;
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
