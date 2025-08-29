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

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

/**
 * This is a workaround for the issue reported in https://issues.apache.org/jira/browse/THRIFT-5732
 * and can be removed once that issue is fixed.
 */
public class AccumuloTFramedTransportFactory extends TFramedTransport.Factory {

  private final int maxMessageSize;

  public AccumuloTFramedTransportFactory(int maxMessageSize) {
    super(maxMessageSize);
    this.maxMessageSize = maxMessageSize;
  }

  @Override
  public TTransport getTransport(TTransport base) throws TTransportException {
    // The input parameter "base" is typically going to be a TSocket implementation
    // that represents a connection between two Accumulo endpoints (client-server,
    // or server-server). The base transport has a maxMessageSize which defaults to
    // 100MB. The FramedTransport that is created by this factory adds a header to
    // the message with payload size information. The FramedTransport has a default
    // frame size of 16MB, but the TFramedTransport constructor sets the frame size
    // to the frame size set on the underlying transport ("base" in this case").
    // According to current Thrift docs, a message has to fit into 1 frame, so the
    // frame size will be set to the value that is lower. Prior to this class being
    // created, we were only setting the frame size, so messages were capped at 100MB
    // because that's the default maxMessageSize. Here we are setting the maxMessageSize
    // and maxFrameSize to the same value on the "base" transport so that when the
    // TFramedTransport object is created, it ends up using the values that we want.
    base.getConfiguration().setMaxFrameSize(maxMessageSize);
    base.getConfiguration().setMaxMessageSize(maxMessageSize);
    return super.getTransport(base);
  }

}
