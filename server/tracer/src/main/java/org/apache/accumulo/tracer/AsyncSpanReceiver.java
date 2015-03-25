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
package org.apache.accumulo.tracer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.tracer.thrift.Annotation;
import org.apache.accumulo.tracer.thrift.RemoteSpan;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.TimelineAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deliver Span information periodically to a destination.
 * <ul>
 * <li>Send host and service information with the span.
 * <li>Cache Destination objects by some key that can be extracted from the span.
 * <li>Can be used to queue spans up for delivery over RPC, or for saving into a file.
 * </ul>
 */
public abstract class AsyncSpanReceiver<SpanKey,Destination> implements SpanReceiver {

  private static final Logger log = LoggerFactory.getLogger(AsyncSpanReceiver.class);

  public static final String SEND_TIMER_MILLIS = "send.timer.millis";

  private final Map<SpanKey,Destination> clients = new HashMap<SpanKey,Destination>();

  protected String host = null;
  protected String service = null;

  protected abstract Destination createDestination(SpanKey key) throws Exception;

  protected abstract void send(Destination resource, RemoteSpan span) throws Exception;

  protected abstract SpanKey getSpanKey(Map<ByteBuffer,ByteBuffer> data);

  Timer timer = new Timer("SpanSender", true);
  protected final AbstractQueue<RemoteSpan> sendQueue = new ConcurrentLinkedQueue<RemoteSpan>();
  int maxQueueSize = 5000;

  // Visible for testing
  AsyncSpanReceiver() {}

  public AsyncSpanReceiver(HTraceConfiguration conf) {
    host = conf.get(DistributedTrace.TRACE_HOST_PROPERTY, host);
    if (host == null) {
      try {
        host = InetAddress.getLocalHost().getCanonicalHostName().toString();
      } catch (UnknownHostException e) {
        host = "unknown";
      }
    }
    service = conf.get(DistributedTrace.TRACE_SERVICE_PROPERTY, service);
    maxQueueSize = conf.getInt(DistributedTrace.TRACE_QUEUE_SIZE_PROPERTY, maxQueueSize);

    int millis = conf.getInt(SEND_TIMER_MILLIS, 1000);
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

  protected void sendSpans() {
    while (!sendQueue.isEmpty()) {
      boolean sent = false;
      RemoteSpan s = sendQueue.peek();
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
          log.warn("Got error sending to " + dest + ", refreshing client", ex);
          clients.remove(dest);
        }
      }
      if (!sent)
        break;
    }
  }

  public static Map<ByteBuffer,ByteBuffer> convertToByteBuffers(Map<byte[],byte[]> bytesMap) {
    if (bytesMap == null)
      return null;
    Map<ByteBuffer,ByteBuffer> result = new HashMap<ByteBuffer,ByteBuffer>();
    for (Entry<byte[],byte[]> bytes : bytesMap.entrySet()) {
      result.put(ByteBuffer.wrap(bytes.getKey()), ByteBuffer.wrap(bytes.getValue()));
    }
    return result;
  }

  public static List<Annotation> convertToAnnotations(List<TimelineAnnotation> annotations) {
    if (annotations == null)
      return null;
    List<Annotation> result = new ArrayList<Annotation>();
    for (TimelineAnnotation annotation : annotations) {
      result.add(new Annotation(annotation.getTime(), annotation.getMessage()));
    }
    return result;
  }

  @Override
  public void receiveSpan(Span s) {
    Map<ByteBuffer,ByteBuffer> data = convertToByteBuffers(s.getKVAnnotations());

    SpanKey dest = getSpanKey(data);
    if (dest != null) {
      List<Annotation> annotations = convertToAnnotations(s.getTimelineAnnotations());
      if (sendQueue.size() > maxQueueSize) {
         return;
      }
      sendQueue.add(new RemoteSpan(host, service == null ? s.getProcessId() : service, s.getTraceId(), s.getSpanId(), s.getParentId(), s.getStartTimeMillis(),
          s.getStopTimeMillis(), s.getDescription(), data, annotations));
    }
  }

  @Override
  public void close() {
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
