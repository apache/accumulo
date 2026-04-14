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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolDecorator;

public class AccumuloTMultiplexedProtocol extends TProtocolDecorator {
  private final RpcService rpcService;

  /**
   * Wrap the specified protocol, allowing it to be used to communicate with a multiplexing server.
   * The <code>rpcService</code> is required as it is written to the end of the message so that the
   * multiplexing server can broker the function call to the proper service.
   *
   * @param protocol Your communication protocol of choice, e.g. <code>TBinaryProtocol</code>.
   * @param rpcService The RpcService communicating via this protocol.
   */
  public AccumuloTMultiplexedProtocol(TProtocol protocol, RpcService rpcService) {
    super(protocol);
    this.rpcService = rpcService;
  }

  /**
   * Appends the RpcService shortId to the message body.
   *
   * @param tMessage The original message.
   * @throws TException Passed through from wrapped <code>TProtocol</code> instance.
   */
  @Override
  public void writeMessageBegin(TMessage tMessage) throws TException {
    if (tMessage.type == TMessageType.CALL || tMessage.type == TMessageType.ONEWAY) {
      super.writeMessageBegin(new TMessage(tMessage.name, tMessage.type, tMessage.seqid));
      super.writeByte(this.rpcService.getShortId());
    } else {
      super.writeMessageBegin(tMessage);
    }
  }
}
