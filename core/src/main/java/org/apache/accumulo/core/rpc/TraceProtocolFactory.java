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
package org.apache.accumulo.core.rpc;

import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

/**
 * {@link org.apache.thrift.protocol.TCompactProtocol.Factory} implementation which uses a protocol
 * which traces
 */
public class TraceProtocolFactory extends TCompactProtocol.Factory {
  private static final long serialVersionUID = 1L;

  @Override
  public TProtocol getProtocol(TTransport trans) {
    return new TCompactProtocol(trans) {
      private TraceScope span = null;

      @Override
      public void writeMessageBegin(TMessage message) throws TException {
        span = Trace.startSpan("client:" + message.name);
        super.writeMessageBegin(message);
      }

      @Override
      public void writeMessageEnd() throws TException {
        super.writeMessageEnd();
        span.close();
      }
    };
  }
}
