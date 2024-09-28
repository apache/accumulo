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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.server.metrics.ThriftMetrics;
import org.apache.thrift.TAsyncProcessor;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.AbstractNonblockingServer.AsyncFrameBuffer;

import com.google.common.base.Preconditions;

/**
 * A {@link TProcessor} which tracks the duration of an RPC and adds it to the metrics subsystem.
 */
public abstract class TimedProcessor implements TProcessor {

  protected final TProcessor other;
  protected final ThriftMetrics thriftMetrics;
  protected long idleStart;

  protected TimedProcessor(final TProcessor next, final MetricsInfo metricsInfo) {
    this.other = next;
    thriftMetrics = new ThriftMetrics();
    metricsInfo.addMetricsProducers(thriftMetrics);
    idleStart = System.nanoTime();
  }

  @Override
  public void process(TProtocol in, TProtocol out) throws TException {
    time(() -> other.process(in, out));
  }

  public boolean isAsync() {
    return false;
  }

  protected void time(ThriftRunnable runnable) throws TException {
    long processStart = System.nanoTime();
    thriftMetrics.addIdle(NANOSECONDS.toMillis(processStart - idleStart));
    try {
      runnable.run();
    } finally {
      // set idle to now, calc time in process
      idleStart = System.nanoTime();
      thriftMetrics.addExecute(NANOSECONDS.toMillis(idleStart - processStart));
    }
  }

  public static class SyncTimedProcessor extends TimedProcessor {
    protected SyncTimedProcessor(TProcessor next, MetricsInfo metricsInfo) {
      super(next, metricsInfo);
    }
  }

  public static class AsyncTimedProcessor extends TimedProcessor implements TAsyncProcessor {
    private final TAsyncProcessor other;

    public AsyncTimedProcessor(TAsyncProcessor next, MetricsInfo metricsInfo) {
      super(validate(next), metricsInfo);
      this.other = next;
    }

    @Override
    public boolean isAsync() {
      return true;
    }

    @Override
    public void process(AsyncFrameBuffer fb) throws TException {
      this.other.process(fb);
    }

    private static TProcessor validate(TAsyncProcessor other) {
      Preconditions.checkArgument(other instanceof TProcessor,
          "Async processor must also implement TProcessor");
      return (TProcessor) other;
    }
  }

  protected interface ThriftRunnable {
    void run() throws TException;
  }
}
