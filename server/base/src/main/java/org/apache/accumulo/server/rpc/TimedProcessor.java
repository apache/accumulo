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
package org.apache.accumulo.server.rpc;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.server.metrics.ThriftMetrics;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;

/**
 * A {@link TProcessor} which tracks the duration of an RPC and adds it to the metrics subsystem.
 */
public class TimedProcessor implements TProcessor {

  private final TProcessor other;
  private final ThriftMetrics thriftMetrics;
  private long idleStart = 0;

  public TimedProcessor(AccumuloConfiguration conf, TProcessor next, String serverName,
      String threadName) {
    this(next, serverName, threadName);
  }

  public TimedProcessor(TProcessor next, String serverName, String threadName) {
    this.other = next;
    thriftMetrics = new ThriftMetrics(serverName, threadName);
    MetricsUtil.initializeProducers(thriftMetrics);
    idleStart = System.currentTimeMillis();
  }

  @Override
  public void process(TProtocol in, TProtocol out) throws TException {
    long now = System.currentTimeMillis();
    thriftMetrics.addIdle(now - idleStart);
    try {
      other.process(in, out);
    } finally {
      idleStart = System.currentTimeMillis();
      thriftMetrics.addExecute(idleStart - now);
    }
  }
}
