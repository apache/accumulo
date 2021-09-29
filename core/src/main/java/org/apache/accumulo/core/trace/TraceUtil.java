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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Objects;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;

public class TraceUtil {

  public static final Logger LOG = LoggerFactory.getLogger(TraceUtil.class);

  public static final String INSTRUMENTATION_NAME = "io.opentelemetry.contrib.accumulo";

  private static final String SPAN_FORMAT = "%s::%s";

  private static Tracer instance = null;
  private static boolean tracing = false;

  public static void initializeTracer(AccumuloConfiguration conf) throws Exception {
    if (instance == null) {
      // Allow user to specify a class that will configure and return
      // an instance of OpenTelemetry
      String factoryClass = conf.get(Property.GENERAL_OPENTELEMETRY_FACTORY);
      if (factoryClass != null && !factoryClass.isEmpty()) {
        Class<? extends OpenTelemetryFactory> clazz =
            (Class<? extends OpenTelemetryFactory>) ClassLoaderUtil.loadClass(factoryClass,
                OpenTelemetryFactory.class);
        OpenTelemetryFactory factory = clazz.getDeclaredConstructor().newInstance();
        OpenTelemetry ot = factory.getOpenTelemetry();
        GlobalOpenTelemetry.set(ot);
        LOG.info("OpenTelemetry configured and set from {}", clazz);
      }

      // Get the Tracer from the global OpenTelemetry object. This could have
      // been set from:
      //
      // a. the code above
      // b. the java agent (https://github.com/open-telemetry/opentelemetry-java-instrumentation)
      // c. or via some other mechanism (for example an application that uses the Accumulo client
      // code)
      //
      // TODO: Is there a way to get our version to pass to getTracer() ?
      instance = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
      tracing = (!instance.equals(OpenTelemetry.noop().getTracer(INSTRUMENTATION_NAME)));
      LOG.info("Tracer is: {}", instance.getClass());
    } else {
      LOG.warn("Tracer already initialized.");
    }
  }

  /**
   * @return the Tracer set on the GlobalOpenTelemetry object
   */
  private static Tracer getTracer() {
    if (Objects.isNull(instance)) {
      LOG.warn("initializeTracer not called, using GlobalOpenTelemetry.getTracer()");
      instance = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
      tracing = (!instance.equals(OpenTelemetry.noop().getTracer(INSTRUMENTATION_NAME)));
      LOG.info("Tracer is: {}", instance.getClass());
    }
    return instance;
  }

  /**
   * @return true if an OpenTelemetry Tracer implementation has been set, false if the NoOp Tracer
   *         is being used.
   */
  public static boolean isTracing() {
    return tracing;
  }

  public static Span createSpan(Class<?> caller, String spanName, SpanKind kind) {
    return createSpan(caller, spanName, kind, null, false, null);
  }

  public static Span createSpan(Class<?> caller, String spanName, SpanKind kind, Context parent) {
    return createSpan(caller, spanName, kind, null, false, parent);
  }

  public static Span createSpan(Class<?> caller, String spanName, SpanKind kind,
      Map<String,String> attributes) {
    return createSpan(caller, spanName, kind, attributes, false, null);
  }

  public static Span createSpan(Class<?> caller, String spanName, SpanKind kind,
      Map<String,String> attributes, boolean setNoParent, Context parent) {
    final String name = String.format(SPAN_FORMAT, caller.getSimpleName(), spanName);
    final SpanBuilder builder = getTracer().spanBuilder(name);
    builder.setSpanKind(kind);
    if (attributes != null) {
      attributes.forEach((k, v) -> builder.setAttribute(k, v));
    }
    if (setNoParent) {
      builder.setNoParent();
    } else if (parent != null) {
      builder.setParent(parent);
    }
    return builder.startSpan();
  }

  /**
   * Record that an Exception occurred in the code covered by a Span
   *
   * @param span
   *          the span
   * @param e
   *          the exception
   * @param rethrown
   *          whether the exception is subsequently re-thrown
   */
  public static void setException(Span span, Throwable e, boolean rethrown) {
    if (tracing) {
      span.setStatus(StatusCode.ERROR);
      span.recordException(e,
          Attributes.builder().put(SemanticAttributes.EXCEPTION_TYPE, e.getClass().getName())
              .put(SemanticAttributes.EXCEPTION_MESSAGE, e.getMessage())
              .put(SemanticAttributes.EXCEPTION_ESCAPED, rethrown).build());
    }
  }

  /**
   * Obtain {@link org.apache.accumulo.core.trace.thrift.TInfo} for the current context. This is
   * used to send the current trace information to a remote process
   */
  public static TInfo traceInfo() {
    TInfo tinfo = new TInfo();
    W3CTraceContextPropagator.getInstance().inject(Context.current(), tinfo,
        (carrier, key, value) -> carrier.putToHeaders(key, value));
    return tinfo;
  }

  /**
   * Returns a newly created Context from the TInfo object sent by a remote process. The Context can
   * then be used in this process to continue the tracing. The Context is used like:
   *
   * <pre>
   * Context remoteCtx = TracerFactory.getContext(tinfo);
   * Span span = TracerFactory.getTracer().spanBuilder(name).setParent(remoteCtx).startSpan()
   * </pre>
   *
   * @param tinfo
   *          tracing information
   * @return Context
   */
  public static Context getContext(TInfo tinfo) {
    return W3CTraceContextPropagator.getInstance().extract(Context.current(), tinfo,
        new TextMapGetter<TInfo>() {
          @Override
          public Iterable<String> keys(TInfo carrier) {
            return carrier.getHeaders().keySet();
          }

          @Override
          public String get(TInfo carrier, String key) {
            if (carrier.getHeaders() == null) {
              return null;
            }
            return carrier.getHeaders().get(key);
          }
        });
  }

  /**
   * To move trace data from client to server, the RPC call must be annotated to take a TInfo object
   * as its first argument. The user can simply pass null, so long as they wrap their Client and
   * Service objects with these functions.
   *
   * <pre>
   * Trace.on(&quot;remoteMethod&quot;);
   * Iface c = new Client();
   * c = TracerFactory.wrapClient(c);
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
      Span span = createSpan(instance.getClass(), "client:" + method.getName(), SpanKind.CLIENT);
      try (Scope scope = span.makeCurrent()) {
        return method.invoke(instance, args);
      } catch (InvocationTargetException ex) {
        span.recordException(ex.getCause(),
            Attributes.builder().put("exception.message", ex.getCause().getMessage())
                .put("exception.escaped", true).build());
        throw ex.getCause();
      } catch (Exception e) {
        span.recordException(e, Attributes.builder().put("exception.message", e.getMessage())
            .put("exception.escaped", true).build());
        throw e;
      } finally {
        span.end();
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
        TInfo tinfo = (TInfo) args[0];
        getContext(tinfo).makeCurrent();
        Span span = createSpan(instance.getClass(), method.getName(), SpanKind.SERVER);
        try (Scope scope = span.makeCurrent()) {
          return method.invoke(instance, args);
        } catch (Exception e) {
          span.recordException(e, Attributes.builder().put("exception.message", e.getMessage())
              .put("exception.escaped", true).build());
          throw e;
        } finally {
          span.end();
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
