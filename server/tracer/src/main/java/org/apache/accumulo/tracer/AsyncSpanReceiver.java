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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

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

  public static final String SEND_TIMER_MILLIS = "tracer.send.timer.millis";
  public static final String QUEUE_SIZE = "tracer.queue.size";
  public static final String SPAN_MIN_MS = "tracer.span.min.ms";

  private final Map<SpanKey,Destination> clients = new HashMap<>();

  protected String host = null;
  protected String service = null;

  protected abstract Destination createDestination(SpanKey key) throws Exception;

  protected abstract void send(Destination resource, RemoteSpan span) throws Exception;

  protected abstract SpanKey getSpanKey(Map<String,String> data);

  Timer timer = new Timer("SpanSender", true);
  protected final AbstractQueue<RemoteSpan> sendQueue = new ConcurrentLinkedQueue<>();
  protected final AtomicInteger sendQueueSize = new AtomicInteger(0);
  int maxQueueSize = 5000;
  long lastNotificationOfDroppedSpans = 0;
  int minSpanSize = 1;

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
    maxQueueSize = conf.getInt(QUEUE_SIZE, maxQueueSize);
    minSpanSize = conf.getInt(SPAN_MIN_MS, minSpanSize);

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
            sendQueueSize.decrementAndGet();
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

  public static Map<String,String> convertToStrings(Map<byte[],byte[]> bytesMap) {
    if (bytesMap == null)
      return null;
    Map<String,String> result = new HashMap<>();
    for (Entry<byte[],byte[]> bytes : bytesMap.entrySet()) {
      result.put(new String(bytes.getKey(), UTF_8), new String(bytes.getValue(), UTF_8));
    }
    return result;
  }

  public static List<Annotation> convertToAnnotations(List<TimelineAnnotation> annotations) {
    if (annotations == null)
      return null;
    List<Annotation> result = new ArrayList<>();
    for (TimelineAnnotation annotation : annotations) {
      result.add(new Annotation(annotation.getTime(), annotation.getMessage()));
    }
    return result;
  }

  @Override
  public void receiveSpan(Span s) {
    if (s.getStopTimeMillis() - s.getStartTimeMillis() < minSpanSize) {
      return;
    }

    Map<String,String> data = convertToStrings(s.getKVAnnotations());

    SpanKey dest = getSpanKey(data);
    if (dest != null) {
      List<Annotation> annotations = convertToAnnotations(s.getTimelineAnnotations());
      if (sendQueueSize.get() > maxQueueSize) {
        long now = System.currentTimeMillis();
        if (now - lastNotificationOfDroppedSpans > 60 * 1000) {
          log.warn("Tracing spans are being dropped because there are already {} spans queued for delivery.\n"
              + "This does not affect performance, security or data integrity, but distributed tracing information is being lost.", maxQueueSize);
          lastNotificationOfDroppedSpans = now;
        }
        return;
      }
      sendQueue.add(new RemoteSpan(host, service == null ? s.getProcessId() : service, s.getTraceId(), s.getSpanId(), s.getParentId(), s.getStartTimeMillis(),
          s.getStopTimeMillis(), s.getDescription(), data, annotations));
      sendQueueSize.incrementAndGet();
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
