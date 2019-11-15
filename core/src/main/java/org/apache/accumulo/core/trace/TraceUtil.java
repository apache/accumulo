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
package org.apache.accumulo.core.trace;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.NullScope;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.SpanReceiverBuilder;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;
import org.apache.htrace.impl.CountSampler;
import org.apache.htrace.impl.ProbabilitySampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for tracing within Accumulo. Not intended for client use!
 */
public class TraceUtil {

  private static final Logger log = LoggerFactory.getLogger(TraceUtil.class);
  public static final String TRACE_HOST_PROPERTY = "trace.host";
  public static final String TRACE_SERVICE_PROPERTY = "trace.service";
  public static final String TRACER_ZK_HOST = "tracer.zookeeper.host";
  public static final String TRACER_ZK_TIMEOUT = "tracer.zookeeper.timeout";
  public static final String TRACER_ZK_PATH = "tracer.zookeeper.path";
  private static final HashSet<SpanReceiver> receivers = new HashSet<>();

  /**
   * Enable tracing by setting up SpanReceivers for the current process. If host name is null, it
   * will be determined. If service name is null, the simple name of the class will be used.
   * Properties required in the client configuration include
   * {@link org.apache.accumulo.core.conf.ClientProperty#TRACE_SPAN_RECEIVERS} and any properties
   * specific to the span receiver.
   */
  public static void enableClientTraces(String hostname, String service, Properties properties) {
    // @formatter:off
      enableTracing(hostname, service,
          ClientProperty.TRACE_SPAN_RECEIVERS.getValue(properties),
          ClientProperty.INSTANCE_ZOOKEEPERS.getValue(properties),
          ConfigurationTypeHelper
              .getTimeInMillis(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getValue(properties)),
          ClientProperty.TRACE_ZOOKEEPER_PATH.getValue(properties),
          ClientProperty.toMap(
              ClientProperty.getPrefix(properties, ClientProperty.TRACE_SPAN_RECEIVER_PREFIX)));
      // @formatter:on
  }

  /**
   * Enable tracing by setting up SpanReceivers for the current process. If host name is null, it
   * will be determined. If service name is null, the simple name of the class will be used.
   */
  public static void enableServerTraces(String hostname, String service,
      AccumuloConfiguration conf) {
    // @formatter:off
      enableTracing(hostname, service,
          conf.get(Property.TRACE_SPAN_RECEIVERS),
          conf.get(Property.INSTANCE_ZK_HOST),
          conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT),
          conf.get(Property.TRACE_ZK_PATH),
          conf.getAllPropertiesWithPrefix(Property.TRACE_SPAN_RECEIVER_PREFIX));
      // @formatter:on
  }

  private static void enableTracing(String hostname, String service, String spanReceivers,
      String zookeepers, long timeout, String zkPath, Map<String,String> spanReceiverProps) {

    Map<String,
        String> htraceConfigProps = spanReceiverProps.entrySet().stream().collect(Collectors.toMap(
            k -> String.valueOf(k).substring(Property.TRACE_SPAN_RECEIVER_PREFIX.getKey().length()),
            v -> String.valueOf(v), (a, b) -> {
              throw new AssertionError("duplicate can't happen");
            }, HashMap::new));
    htraceConfigProps.put(TRACER_ZK_HOST, zookeepers);
    htraceConfigProps.put(TRACER_ZK_TIMEOUT, Long.toString(timeout));
    htraceConfigProps.put(TRACER_ZK_PATH, zkPath);
    if (hostname != null) {
      htraceConfigProps.put(TRACE_HOST_PROPERTY, hostname);
    }
    if (service != null) {
      htraceConfigProps.put(TRACE_SERVICE_PROPERTY, service);
    }
    ShutdownHookManager.get().addShutdownHook(() -> {
      disable();
    }, 0);
    synchronized (receivers) {
      if (!receivers.isEmpty()) {
        log.info("Already loaded span receivers, enable tracing does not need to be called again");
        return;
      }
      if (spanReceivers == null) {
        return;
      }
      Stream.of(spanReceivers.split(",")).map(String::trim).filter(s -> !s.isEmpty())
          .forEach(className -> {
            HTraceConfiguration htraceConf = HTraceConfiguration.fromMap(htraceConfigProps);
            SpanReceiverBuilder builder = new SpanReceiverBuilder(htraceConf);
            SpanReceiver rcvr = builder.spanReceiverClass(className.trim()).build();
            if (rcvr == null) {
              log.warn("Failed to load SpanReceiver {}", className);
            } else {
              receivers.add(rcvr);
              log.debug("SpanReceiver {} was loaded successfully.", className);
            }
          });
      for (SpanReceiver rcvr : receivers) {
        Trace.addReceiver(rcvr);
      }
    }
  }

  /**
   * Disable tracing by closing SpanReceivers for the current process.
   */
  public static void disable() {
    synchronized (receivers) {
      receivers.forEach(rcvr -> {
        try {
          rcvr.close();
        } catch (IOException e) {
          log.warn("Unable to close SpanReceiver correctly: {}", e.getMessage(), e);
        }
      });
      receivers.clear();
    }
  }

  /**
   * Continue a trace by starting a new span with a given parent and description.
   */
  public static TraceScope trace(TInfo info, String description) {
    return info.traceId == 0 ? NullScope.INSTANCE
        : Trace.startSpan(description, new TraceInfo(info.traceId, info.parentId));
  }

  private static final TInfo DONT_TRACE = new TInfo(0, 0);

  /**
   * Obtain {@link org.apache.accumulo.core.trace.thrift.TInfo} for the current span.
   */
  public static TInfo traceInfo() {
    Span span = Trace.currentSpan();
    if (span != null) {
      return new TInfo(span.getTraceId(), span.getSpanId());
    }
    return DONT_TRACE;
  }

  public static CountSampler countSampler(long frequency) {
    return new CountSampler(HTraceConfiguration.fromMap(Collections
        .singletonMap(CountSampler.SAMPLER_FREQUENCY_CONF_KEY, Long.toString(frequency))));
  }

  public static ProbabilitySampler probabilitySampler(double fraction) {
    return new ProbabilitySampler(HTraceConfiguration.fromMap(Collections
        .singletonMap(ProbabilitySampler.SAMPLER_FRACTION_CONF_KEY, Double.toString(fraction))));
  }

  /**
   * To move trace data from client to server, the RPC call must be annotated to take a TInfo object
   * as its first argument. The user can simply pass null, so long as they wrap their Client and
   * Service objects with these functions.
   *
   * <pre>
   * Trace.on(&quot;remoteMethod&quot;);
   * Iface c = new Client();
   * c = TraceWrap.client(c);
   * c.remoteMethod(null, arg2, arg3);
   * Trace.off();
   * </pre>
   *
   * The wrapper will see the annotated method and send or re-establish the trace information.
   *
   * Note that the result of these calls is a Proxy object that conforms to the basic interfaces,
   * but is not your concrete instance.
   */
  public static <T> T wrapClient(final T instance) {
    InvocationHandler handler = (obj, method, args) -> {
      if (args == null || args.length < 1 || args[0] != null) {
        return method.invoke(instance, args);
      }
      if (TInfo.class.isAssignableFrom(method.getParameterTypes()[0])) {
        args[0] = traceInfo();
      }
      try (TraceScope span = Trace.startSpan("client:" + method.getName())) {
        return method.invoke(instance, args);
      } catch (InvocationTargetException ex) {
        throw ex.getCause();
      }
    };
    return wrapRpc(handler, instance);
  }

  public static <T> T wrapService(final T instance) {
    InvocationHandler handler = (obj, method, args) -> {
      try {
        if (args == null || args.length < 1 || args[0] == null || !(args[0] instanceof TInfo)) {
          return method.invoke(instance, args);
        }
        try (TraceScope span = trace((TInfo) args[0], method.getName())) {
          return method.invoke(instance, args);
        }
      } catch (InvocationTargetException ex) {
        throw ex.getCause();
      }
    };
    return wrapRpc(handler, instance);
  }

  private static <T> T wrapRpc(final InvocationHandler handler, final T instance) {
    @SuppressWarnings("unchecked")
    T proxiedInstance = (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(),
        instance.getClass().getInterfaces(), handler);
    return proxiedInstance;
  }

}
