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

import java.util.Objects;

import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolDecorator;
import org.apache.thrift.protocol.TProtocolException;

/**
 * AccumuloTMultiplexedProcessor is a {@link TMultiplexedProcessor} variant that uses the
 * {@link AccumuloTMultiplexedProtocol} to register additional processors and then forward messages
 * based on a single byte entry instead of having to create substrings of the original message.
 */
public class AccumuloTMultiplexedProcessor extends TMultiplexedProcessor {

  // Support enough slots as RPC service types.
  private final TProcessor[] PROCESSORS = new TProcessor[RpcService.values().length];

  public void registerProcessor(RpcService service, TProcessor processor) {
    PROCESSORS[Byte.toUnsignedInt(service.getShortId())] = Objects.requireNonNull(processor,
        "processor must not be null for RPCService: " + service.name());
  }

  /**
   * Default processors are not supported as all AccumuloMultiplexedProcessors will have multiple
   * processors.
   *
   * @param processor the service which will be ignored.
   * @throws UnsupportedOperationException because this operation is not supported.
   */
  @Override
  public void registerDefault(TProcessor processor) {
    throw new UnsupportedOperationException(
        this.getClass().getSimpleName() + " does not use default processors in any server type");
  }

  /**
   * Registering processors with strings is not supported
   *
   * @param processor the service which will be ignored.
   * @throws UnsupportedOperationException because this operation is not supported.
   */
  @Override
  public void registerProcessor(String string, TProcessor processor) {
    throw new UnsupportedOperationException(this.getClass().getSimpleName()
        + " does not allow processors to be registered via strings");
  }

  /**
   * This implementation of <code>process</code> performs the following steps:
   *
   * <ol>
   * <li>Read the beginning of the message.
   * <li>Reads a single byte at the end of the message.
   * <li>Uses that byte to locate the appropriate processor.
   * <li>Dispatch to the processor, with a decorated instance of TProtocol that allows
   * readMessageBegin() to return the original TMessage.
   * </ol>
   *
   * @throws TProtocolException If the message type is not CALL or ONEWAY, if the service name was
   *         not found in the message, or if the service name was not found in the service map. You
   *         called {@link AccumuloTMultiplexedProcessor#registerProcessor(RpcService, TProcessor)
   *         registerProcessor} during initialization, right? :)
   */
  @Override
  public void process(TProtocol iprot, TProtocol oprot) throws TException {
    /*
     * Use the actual underlying protocol (e.g. TBinaryProtocol) to read the message header. This
     * pulls the message "off the wire", which we'll deal with at the end of this method.
     */
    TMessage message = iprot.readMessageBegin();

    if (message.type != TMessageType.CALL && message.type != TMessageType.ONEWAY) {
      throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED,
          "This should not have happened!?");
    }

    // Read the rpcServiceShortId from the message body
    byte serviceId = iprot.readByte();

    int index = Byte.toUnsignedInt(serviceId);
    TProcessor actualProcessor = PROCESSORS[index];

    if (actualProcessor == null) {
      throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED,
          "RpcService shortId not found after message: " + message.name + ".  Did you "
              + "forget to use the " + AccumuloTMultiplexedProtocol.class.getSimpleName()
              + " in your client?");
    }

    // Dispatch processing to the stored processor
    actualProcessor.process(new StoredMessageProtocol(iprot, message), oprot);
  }

  private static class StoredMessageProtocol extends TProtocolDecorator {

    TMessage pendingMessage;

    public StoredMessageProtocol(TProtocol delegate, TMessage message) {
      super(delegate);
      this.pendingMessage = message;
    }

    @Override
    public TMessage readMessageBegin() throws TException {
      if (pendingMessage != null) {
        TMessage m = pendingMessage;
        pendingMessage = null; // Release for GC; subsequent calls fall through
        return m;
      }
      return super.readMessageBegin();
    }
  }
}
