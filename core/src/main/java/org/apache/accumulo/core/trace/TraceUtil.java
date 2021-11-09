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

import static org.apache.accumulo.core.Constants.APPNAME;
import static org.apache.accumulo.core.Constants.VERSION;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.trace.OpenTelemetryFactory;
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

  private static final String SPAN_FORMAT = "%s::%s";

  private static Tracer instance = null;
  private static boolean tracing = false;

  private static void initializeInternals(OpenTelemetry ot) {
    instance = ot.getTracer(APPNAME, VERSION);
    tracing = !ot.equals(OpenTelemetry.noop());
    LOG.info("Trace enabled: {}, OpenTelemetry instance: {}, Tracer instance: {}", tracing,
        ot.getClass(), instance.getClass());
  }

  /**
   * Initialize TracerUtil using the OpenTelemetry parameter
   */
  public static void initializeTracer(OpenTelemetry ot) {
    if (instance != null) {
      initializeInternals(ot);
    } else {
      LOG.warn("Tracer already initialized.");
    }
  }

  /**
   * Use the property values in the AccumuloConfiguration to call
   * {@link #initializeTracer(boolean, String, String)}
   *
   * @param conf
   *          AccumuloConfiguration
   * @throws Exception
   *           unable to find or load class
   */
  public static void initializeTracer(AccumuloConfiguration conf) throws Exception {
    initializeTracer(conf.getBoolean(Property.GENERAL_OPENTELEMETRY_ENABLED),
        conf.get(Property.GENERAL_OPENTELEMETRY_FACTORY));
  }

  /**
   * If not enabled, the OpenTelemetry implementation will be set to the NoOp implementation. If
   * enabled and a factoryClass is supplied, then we will get the OpenTelemetry instance from the
   * factory class.
   *
   * @param enabled
   *          whether or not tracing is enabled
   * @param factoryClass
   *          name of class to load
   * @throws Exception
   *           unable to find or load class
   */
  public static void initializeTracer(boolean enabled, String factoryClass) throws Exception {
    if (instance == null) {
      OpenTelemetry ot = null;
      if (!enabled) {
        ot = OpenTelemetry.noop();
      } else if (factoryClass != null && !factoryClass.isEmpty()) {
        var clazz = ClassLoaderUtil.loadClass(factoryClass, OpenTelemetryFactory.class);
        OpenTelemetryFactory factory = clazz.getDeclaredConstructor().newInstance();
        ot = factory.get();
        LOG.info("OpenTelemetry configured and set from {}", clazz);
      } else {
        ot = GlobalOpenTelemetry.get();
      }
      initializeInternals(ot);
    } else {
      LOG.warn("Tracer already initialized.");
    }
  }

  /**
   * @return the Tracer set on the GlobalOpenTelemetry object
   */
  private static Tracer getTracer() {
    if (instance == null) {
      LOG.warn("initializeTracer not called, using GlobalOpenTelemetry.getTracer()");
      instance = GlobalOpenTelemetry.getTracer(APPNAME, VERSION);
      tracing = !instance.equals(OpenTelemetry.noop().getTracer(APPNAME, VERSION));
      LOG.info("Trace enabled: {}, Tracer is: {}", tracing, instance.getClass());
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

  public static Span startSpan(Class<?> caller, String spanName) {
    return startSpan(caller, spanName, null, null, null);
  }

  public static Span startSpan(Class<?> caller, String spanName, Map<String,String> attributes) {
    return startSpan(caller, spanName, null, attributes, null);
  }

  public static Span startClientSpan(Class<?> caller, String spanName) {
    return startSpan(caller, spanName, SpanKind.CLIENT, null, null);
  }

  public static Span startServerSpan(Class<?> caller, String spanName, TInfo tinfo) {
    return startSpan(caller, spanName, SpanKind.SERVER, null, getContext(tinfo));
  }

  private static Span startSpan(Class<?> caller, String spanName, SpanKind kind,
      Map<String,String> attributes, Context parent) {
    final String name = String.format(SPAN_FORMAT, caller.getSimpleName(), spanName);
    final SpanBuilder builder = getTracer().spanBuilder(name);
    if (kind != null) {
      builder.setSpanKind(kind);
    }
    if (attributes != null) {
      attributes.forEach(builder::setAttribute);
    }
    if (parent != null) {
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
  private static Context getContext(TInfo tinfo) {
    return W3CTraceContextPropagator.getInstance().extract(Context.current(), tinfo,
        new TextMapGetter<TInfo>() {
          @Override
          public Iterable<String> keys(TInfo carrier) {
            if (carrier.getHeaders() == null) {
              return null;
            }
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

  public static <T> T wrapService(final T instance) {
    InvocationHandler handler = (obj, method, args) -> {
      if (args == null || args.length < 1 || args[0] == null || !(args[0] instanceof TInfo)) {
        try {
          return method.invoke(instance, args);
        } catch (InvocationTargetException e) {
          throw e.getCause();
        }
      }
      Span span = startServerSpan(instance.getClass(), method.getName(), (TInfo) args[0]);
      try (Scope scope = span.makeCurrent()) {
        return method.invoke(instance, args);
      } catch (Exception e) {
        Throwable t = e instanceof InvocationTargetException ? e.getCause() : e;
        setException(span, t, true);
        throw t;
      } finally {
        span.end();
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
