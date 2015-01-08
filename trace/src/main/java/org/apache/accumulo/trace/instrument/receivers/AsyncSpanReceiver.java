/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.trace.instrument.receivers;

import java.util.AbstractQueue;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.accumulo.trace.thrift.RemoteSpan;
import org.apache.log4j.Logger;

/**
 * Deliver Span information periodically to a destination.
 * <ul>
 * <li>Send host and service information with the span.
 * <li>Cache Destination objects by some key that can be extracted from the span.
 * <li>Can be used to queue spans up for delivery over RPC, or for saving into a file.
 * </ul>
 */
public abstract class AsyncSpanReceiver<SpanKey,Destination> implements SpanReceiver {

  private static final Logger log = Logger.getLogger(AsyncSpanReceiver.class);

  private final Map<SpanKey,Destination> clients = new HashMap<SpanKey,Destination>();

  protected final String host;
  protected final String service;

  protected abstract Destination createDestination(SpanKey key) throws Exception;

  protected abstract void send(Destination resource, RemoteSpan span) throws Exception;

  protected abstract SpanKey getSpanKey(Map<String,String> data);

  Timer timer = new Timer("SpanSender", true);
  final AbstractQueue<RemoteSpan> sendQueue = new ConcurrentLinkedQueue<RemoteSpan>();

  public AsyncSpanReceiver(String host, String service, long millis) {
    this.host = host;
    this.service = service;
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          sendSpans();
        } catch (Exception ex) {
          log.warn("Exception sending spans to destination", ex);
        }
      }

    }, millis, millis);
  }

  void sendSpans() {
    while (!sendQueue.isEmpty()) {
      boolean sent = false;
      RemoteSpan s = sendQueue.peek();
      if (s.stop - s.start < 1) {
        synchronized (sendQueue) {
          sendQueue.remove();
          sendQueue.notifyAll();
        }
        continue;
      }
      SpanKey dest = getSpanKey(s.data);
      Destination client = clients.get(dest);
      if (client == null) {
        try {
          clients.put(dest, createDestination(dest));
        } catch (Exception ex) {
          log.warn("Exception creating connection to span receiver", ex);
        }
      }
      if (client != null) {
        try {
          send(client, s);
          synchronized (sendQueue) {
            sendQueue.remove();
            sendQueue.notifyAll();
          }
          sent = true;
        } catch (Exception ex) {
          log.error(ex, ex);
        }
      }
      if (!sent)
        break;
    }
  }

  @Override
  public void span(long traceId, long spanId, long parentId, long start, long stop, String description, Map<String,String> data) {

    SpanKey dest = getSpanKey(data);
    if (dest != null) {
      sendQueue.add(new RemoteSpan(host, service, traceId, spanId, parentId, start, stop, description, data));
    }
  }

  @Override
  public void flush() {
    synchronized (sendQueue) {
      while (!sendQueue.isEmpty()) {
        try {
          sendQueue.wait();
        } catch (InterruptedException e) {
          log.warn("flush interrupted");
          break;
        }
      }
    }
  }

}
